(*
 * Copyright (C) Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)

let _introduceDomain = "@introduceDomain"
let _releaseDomain = "@releaseDomain"

module Client = Xs_client_unix.Client(Xs_transport_unix_client)
let make_client () =
  try
    Client.set_logger (fun s -> Logs.debug (fun m -> m "Xs_client_unix: %s" s));
    Client.make ()
  with e ->
    Logs.err (fun m -> m "Failed to connect to xenstore. The raw error was: %s" (Printexc.to_string e));
    begin match e with
      | Unix.Unix_error(Unix.EACCES, _, _) ->
        Logs.err (fun m -> m "Access to xenstore was denied.");
        let euid = Unix.geteuid () in
        if euid <> 0 then begin
          Logs.err (fun m -> m "My effective uid is %d." euid);
          Logs.err (fun m -> m "Typically xenstore can only be accessed by root (uid 0).");
          Logs.err (fun m -> m "Please switch to root (uid 0) and retry.")
        end
      | Unix.Unix_error(Unix.ECONNREFUSED, _, _) ->
        Logs.err (fun m -> m "Access to xenstore was refused.");
        Logs.err (fun m -> m "This normally indicates that the service is not running.");
        Logs.err (fun m -> m "Please start the xenstore service and retry.")
      | _ -> ()
    end;
    raise e


let lock = Mutex.create ()

let with_lock m n f x =
  let id = Thread.id (Thread.self ()) in
  (* Printf.printf "%d Acquiring lock (%s)\n%!" id n; *)
  Mutex.lock m;
  try
    (* Printf.printf "%d Got lock (%s)\n%!" id n; *)
    let result = f x in
    (* Printf.printf "%d Releasing lock (%s)\n%!" id n; *)
    Mutex.unlock m;
    result
  with e ->
    (* Printf.printf "%d Releasing lock (%s) (in exception handler: e=%s)\n%!" id n (Printexc.to_string e); *)
    Mutex.unlock m;
    raise e

let watches = Queue.create ()
let wcond = Condition.create ()

module WatchQueue = struct  
  let enqueue_watch cb =
    (* Nb, this will be called with the mutex already locked *)
    (* Printf.printf "enqueuing fired watch callback\n%!"; *)
    Queue.add cb watches;
    Condition.broadcast wcond
  
  let start_dequeue_watches_thread () =
    Thread.create (fun () ->
      let rec inner () =
        let q = with_lock lock "dequeue" (fun () ->
          (* Printf.printf "dequeue: List.length !watches=%d\n%!" (Queue.length watches);  *)
          while (Queue.length watches) = 0 do
            (* Printf.printf "%d Releasing lock on condition (%s)\n%!" (Thread.id (Thread.self ())) "dequeue"; *)
            Condition.wait wcond lock;
            (* Printf.printf "%d Got lock again after condition (%s)\n%!" (Thread.id (Thread.self ())) "dequeue"; *)
          done;
          let result = Queue.copy watches in
          Queue.clear watches;
          result
          ) ()
        in
        (* Printf.printf "Queue length: %d\n%!" (Queue.length q); *)
        Queue.iter (fun i ->
          (* Printf.printf "Calling callback...\n%!"; *)
          (* let now = Unix.gettimeofday () in *)
          (try i () with _ -> ());
          (* let thn = Unix.gettimeofday () in *)
          (* Printf.printf "Callback took %f seconds\n%!" (thn -. now) *)
          )
          q;
        inner ()
      in inner ()) ()

end

module Cache = Xscache.Cache(WatchQueue)


let cache = ref Cache.empty
let caching_enabled = ref false
let client = ref None
let special_cb = ref (fun (_path, _token) -> ())
let cache_n = ref 0L

let strip_leading_slash = function | ""::xs -> xs | x -> x
let list_path path = Astring.String.cuts ~sep:"/" path |> strip_leading_slash

let rec ls_lR xs dir =
    Printf.printf ".%!";
    let this = try [ dir, Client.read xs dir ] with _ -> [] in
    let subdirs = try Client.directory xs dir |> List.filter (fun x -> x <> "") |> List.map (fun x -> Filename.concat dir x) with _ -> [] in
    this @ (List.concat (List.map (ls_lR xs) subdirs))

(* Cache synchronisation *)
let cache_conditions = Hashtbl.create 10
let cache_sync_stem = "cachesync:"
let cache_sync_seen = ref (0L)
let cache_sync_stem_len = String.length cache_sync_stem

let process_watch c (path, token) =
  (* Printf.printf "process_watch: %s %s\n%!" path token; *)
  if String.length token > cache_sync_stem_len && String.sub token 0 cache_sync_stem_len = cache_sync_stem then begin
    let n = String.sub token cache_sync_stem_len (String.length token - cache_sync_stem_len) |> Int64.of_string in
    (* Printf.printf "%d process_watch: got cache trigger %s\n%!" (Thread.id (Thread.self ())) token; *)
    with_lock lock "cache trigger" (fun () ->
      cache_sync_seen := n; 
      try
        let condition = Hashtbl.find cache_conditions token in
        (* Printf.printf "%d broadcasting %s\n%!" (Thread.id (Thread.self ())) token; *)
        Condition.broadcast condition
      with e ->
        (* Printf.printf "%d got exception! %s\n%!" (Thread.id (Thread.self ())) (Printexc.to_string e); *)
        ()
      ) ()
  end else begin
  if path = _introduceDomain || path = _releaseDomain then (WatchQueue.enqueue_watch (fun () -> !special_cb (path, token))) else begin
  with_lock lock "process watch" (fun () ->
    let t = !cache in
    let vopt =
      try
        Client.immediate c (fun xs ->
          Some (Client.read xs path))
      with Xs_protocol.Enoent _ -> None
    in
    let path' = list_path path in
    begin
      match vopt with
      | Some v ->
        (* Printf.printf "cache update: %s=%s\n%!" path v; *)
        cache := Cache.write t path' v
      | None ->
        (* Printf.printf "cache update: rm %s\n%!" path; *)
        cache := Cache.rm t path'
    end) ()
  end
end

let get_client () =
  with_lock lock "get_client" (fun () ->
    match !client with
    | None ->
      let c = make_client () in
      client := Some c;
      if !caching_enabled then begin
        Client.set_watch_callback c (process_watch c);
        Client.immediate c (fun xs ->
          Client.watch xs "/" "tok";
          Client.watch xs _introduceDomain "";
          Client.watch xs _releaseDomain "";
          Client.rm xs "/cache";
          let lslr = ls_lR xs "/" in
          Printf.printf "Populating internal cache\n%!";
          let t = List.fold_left (fun t (p,v) ->
            Cache.write t (list_path p) v) Cache.empty lslr in
          Printf.printf "Populated\n%!";
          cache := t;
          let _th = WatchQueue.start_dequeue_watches_thread () in
          ())
      end;
      c
    | Some c ->
      c
   ) ()

let forget_client () = with_lock lock "forget client" (fun () -> client := None) ()


let counter = ref 0l

module StringSet = Set.Make(struct type t = string let compare = String.compare end)

module PathRecorder = struct
  type t = {
    token: string;
    m: Mutex.t;
    mutable accessed_paths: StringSet.t;        (* paths read or written to *)
    mutable watched_paths: StringSet.t;         (* paths being watched *)
  }
  let on t_opt f = match t_opt with | Some t -> f t | None -> () 
  let access t_opt p = on t_opt (fun t -> t.accessed_paths <- StringSet.add p t.accessed_paths)
  let watch t_opt p = on t_opt (fun t -> t.watched_paths <- StringSet.add p t.watched_paths)
  let unwatch t_opt p = on t_opt (fun t -> t.watched_paths <- StringSet.remove p t.watched_paths)
  let clear_access t_opt = on t_opt (fun t -> t.accessed_paths <- StringSet.empty)
  let clear_watches t_opt = on t_opt (fun t -> t.watched_paths <- StringSet.empty)
  let accessed t_opt = match t_opt with | Some t -> t.accessed_paths | None -> StringSet.empty
  let watched t_opt = match t_opt with | Some t -> t.watched_paths | None -> StringSet.empty

  let empty token = { token; m=Mutex.create (); accessed_paths = StringSet.empty; watched_paths = StringSet.empty }
end


module Xs = struct
  type domid = int

  type xsh = {
    directory : string -> string list;
    read : string -> string;
    write : string -> string -> unit;
    writev : string -> (string * string) list -> unit;
    mkdir : string -> unit;
    rm : string -> unit;
    setperms : string -> Xs_protocol.ACL.t -> unit;

    getdomainpath : domid -> string;
    watch : string -> string -> unit;
    unwatch : string -> string -> unit;
    introduce : domid -> nativeint -> int -> unit;
    set_target : domid -> domid -> unit;

    better_watch : (string list * string * (string list -> string -> unit)) list -> unit;
    better_unwatch : (string list * string) list -> unit;

    set_special_watch_cb : (string * string -> unit) -> unit;

    (* Compound operations not corresponding one-to-one with those of Client *)
    mkdirperms : string -> Xs_protocol.ACL.t -> unit;
  }

  let sync h =
    let path = "/cache" in 
    let cond = Condition.create () in
    let n,token = with_lock lock "sync find n"
      (fun () ->
        cache_n := Int64.add 1L !cache_n;
        let n = !cache_n in
        let token = Printf.sprintf "%s%Ld" cache_sync_stem n in
        Hashtbl.replace cache_conditions token cond;
        Client.watch h path token; (* With the lock held so we guarantee to respect ordering of watches *)
        n, token) () in
    (* Printf.printf "%d sync: waiting for cache=%Ld\n%!" (Thread.id (Thread.self ())) n; *)
    let check () = !cache_sync_seen < n in
    with_lock lock "sync check cond" (fun () ->
      while check () do
        (* Printf.printf "%d Releasing lock on condition (%s)\n%!" (Thread.id (Thread.self ())) "sync check cond"; *)
        Condition.wait cond lock;
        (* Printf.printf "%d Got lock again after condition (%s)\n%!" (Thread.id (Thread.self ())) "sync check cond" *)
      done;
      Hashtbl.remove cache_conditions token
      ) ();
    (* Printf.printf "%d sync: cache=%Ld\n%!" (Thread.id (Thread.self ())) n; *)
    Client.unwatch h path token
    

  let gen_ops path_recorder immediate h =
    let path p =
      PathRecorder.access path_recorder p;
      list_path p in
    if !caching_enabled
    then {
      read =
        (fun p ->
          let t,p' = with_lock lock "read" (fun () -> !cache, path p) () in
          match Cache.read t p' with 
          | Some x -> x
          | None -> raise (Xs_protocol.Enoent p));
      directory =
        (fun p ->
          let t,p' = with_lock lock "directory" (fun () -> !cache, path p) () in
          match Cache.directory t p' with
          | Some x -> x
          | None -> raise (Xs_protocol.Enoent p));
      write =
        (fun p v ->
          let p' = with_lock lock "write" (fun () -> path p) () in
          Client.write h p v;
          if immediate then begin
            sync h;
            with_lock lock "verify_cache" (fun () -> 
              match Cache.read !cache p' with
              | Some x -> if x <> v then failwith "Argh"
              | None -> failwith (Printf.sprintf "Argh2 %s %s" p v)) ()
          end
          );
      writev =
        (fun base_path kvs ->
          List.iter (fun (k, v) ->
            let p = base_path ^ "/" ^ k in 
            let _p' = with_lock lock "writev" (fun () -> path p) () in
            Client.write h p v) kvs;
            if immediate then sync h);
      mkdir = 
        (fun p ->
          let _p' = with_lock lock "mkdir" (fun () -> path p) in
          Client.mkdir h p);
      rm =
        (fun p ->
          let _p' = with_lock lock "rm" (fun () -> path p) in
          (try Client.rm h p with Xs_protocol.Enoent _ -> ());
          if immediate then sync h);
      setperms = Client.setperms h;
      getdomainpath = Client.getdomainpath h;
      watch = (fun _p _tok -> ignore(failwith "Unsupported: use better watches!"));
      unwatch = (fun _p _tok -> ignore(failwith "Unsupported: use better watches!"));
      better_watch = (fun watches -> 
        with_lock lock "better watch" (fun () ->
          let single (path,tok,cb) =
            (* Printf.printf "watching [/%s] tok=%s\n%!" (String.concat "/" path) tok; *)
            (match path_recorder with | Some pr -> if pr.PathRecorder.token != tok then failwith "Token invalid" | None -> ());
            PathRecorder.watch path_recorder (Printf.sprintf "/%s" (String.concat "/" path));
            cache := Cache.watch !cache path tok cb
          in
          List.iter single watches) ());
      better_unwatch = (fun watches ->
        with_lock lock "better unwatch" (fun () ->
          let single (path,tok) =
            (* Printf.printf "unwatching [/%s] tok=%s\n%!" (String.concat "/" path) tok; *)
            (match path_recorder with | Some pr -> if pr.PathRecorder.token != tok then failwith "Token invalid" | None -> ());
            PathRecorder.unwatch path_recorder (Printf.sprintf "/%s" (String.concat "/" path));
            cache := Cache.unwatch !cache path tok
          in
          List.iter single watches) ());
      set_special_watch_cb = (fun cb ->
          special_cb := cb
        );
      introduce = Client.introduce h;
      set_target = Client.set_target h;
      mkdirperms = (fun path -> (Client.mkdir h path; Client.setperms h path));
    } else {
    read = Client.read h;
    directory = Client.directory h;
    write = Client.write h;
    writev = (fun base_path -> List.iter (fun (k, v) -> Client.write h (base_path ^ "/" ^ k) v));
    mkdir = Client.mkdir h;
    rm = (fun path -> try Client.rm h path with Xs_protocol.Enoent _ -> ());
    setperms = Client.setperms h;
    getdomainpath = Client.getdomainpath h;
    watch = Client.watch h;
    unwatch = Client.unwatch h;
    introduce = Client.introduce h;
    set_target = Client.set_target h;
    better_watch = (fun _ -> ignore(failwith "Unsupported in uncached xenstore"));
    better_unwatch = (fun _ -> ignore(failwith "Unsupported in uncached xenstore"));
    set_special_watch_cb = (fun _ -> ignore (failwith "Unsupported in uncached xenstore"));
    mkdirperms = (fun path -> (Client.mkdir h path; Client.setperms h path));
    }
  let ops h = gen_ops None true h
  let with_xs f = Client.immediate (get_client ()) (fun h -> f (gen_ops None true h))
  let wait_uncached f = Client.wait (get_client ()) (fun h -> f (gen_ops None true h))
  let transaction _ f =
    let result = Client.transaction (get_client ()) (fun h -> f (gen_ops None false h)) in
    Client.immediate (get_client ()) sync;
    result

  let wait f =
    if not !caching_enabled
    then wait_uncached f
    else begin
        (* We signal the caller via this cancellable task: *)
      let path_recorder = with_lock lock "gen_pathrecorder" (fun () ->
        counter := Int32.succ !counter;
        PathRecorder.empty (Printf.sprintf "%s%ld" "auto_internal:" !counter)) () in
      let xs = gen_ops (Some path_recorder) true (Client.immediate (get_client ()) (fun c -> c)) in
      let unwatch_all () =
        with_lock path_recorder.PathRecorder.m "unwatch_all" (fun () ->
          let watched_paths = with_lock lock "unwatch_all" (fun () -> PathRecorder.watched (Some path_recorder) |> StringSet.elements) () in
          xs.better_unwatch (List.map (fun p -> ((list_path p),path_recorder.PathRecorder.token)) watched_paths)) ()
      in

      let t = Xs_client_unix.Task.make () in
      Xs_client_unix.Task.on_cancel t (fun () -> unwatch_all ());

      (* Adjust the paths we're watching (if necessary) and block (if possible) *)
      let rec watch_fn path tok =
        (* Printf.printf "%d evaluating watch_fn: path=/%s tok=%s\n%!" (Thread.id (Thread.self ())) (String.concat "/" path) tok; *)
        try
          with_lock path_recorder.PathRecorder.m "watch_fn outer" (fun () -> 
            with_lock lock "watch_fn" (fun () -> PathRecorder.clear_access (Some path_recorder)) ();
            let result = f xs in
            Xs_client_unix.Task.wakeup t result) ();
          unwatch_all ()
        with
          | Xs_protocol.Eagain ->
            adjust_paths ()
          | e ->
            Printf.printf "XXXXXXX caught exception %s, cancelling\n%!" (Printexc.to_string e);
            unwatch_all ();
            Xs_client_unix.Task.cancel t
      and adjust_paths () =
        with_lock path_recorder.PathRecorder.m "adjust_paths outer" (fun () ->
          let current_paths,accessed = with_lock lock "adjust_paths" (fun () -> PathRecorder.watched (Some path_recorder), PathRecorder.accessed (Some path_recorder)) () in
          (* Printf.printf "%s current_paths: [%s]  " path_recorder.PathRecorder.token (String.concat ";" (StringSet.elements current_paths)); *)
          (* Paths which weren't read don't need to be watched: *)
          (* Printf.printf "accessed: [%s]  " (String.concat ";" (StringSet.elements accessed)); *)
          let old_paths = StringSet.diff current_paths accessed in
          (* Printf.printf "old_paths: [%s]\n%!" (String.concat ";" (StringSet.elements old_paths)); *)
          xs.better_unwatch (List.map (fun p -> ((list_path p),(path_recorder.PathRecorder.token))) (StringSet.elements old_paths));
          (* Paths which were read do need to be watched: *)
          let new_paths = StringSet.diff accessed current_paths in
          xs.better_watch (List.map (fun p -> ((list_path p),(path_recorder.PathRecorder.token),watch_fn)) (StringSet.elements new_paths))) ();
      in
      watch_fn [] (path_recorder.PathRecorder.token);
      t
    end
end

module Xst = Xs
include Xs
