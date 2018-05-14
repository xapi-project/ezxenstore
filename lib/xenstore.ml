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


let m = Mutex.create ()

let with_lock n f x =
  let id = Thread.id (Thread.self ()) in
  Printf.printf "%d Acquiring lock (%s)\n%!" id n;
  Mutex.lock m;
  try
    Printf.printf "%d Got lock (%s)\n%!" id n;
    let result = f x in
    Printf.printf "%d Releasing lock (%s)\n%!" id n;
    Mutex.unlock m;
    result
  with e ->
    Printf.printf "%d Releasing lock (%s) (in exception handler)\n%!" id n;
    Mutex.unlock m;
    raise e

let watches = ref []
let wcond = Condition.create ()

module WatchQueue = struct  
  let enqueue_watch cb =
    (* Nb, this will be called with the mutex already locked *)
    Printf.printf "enqueuing fired watch callback\n%!";
    watches := cb :: !watches;
    Condition.broadcast wcond
  
  let start_dequeue_watches_thread () =
    Thread.create (fun () ->
      let rec inner () =
        let cbs = with_lock "dequeue" (fun () ->
          Printf.printf "dequeue: List.length !watches=%d\n%!" (List.length !watches); 
          while (List.length !watches) = 0 do
            Printf.printf "%d Releasing lock on condition (%s)\n%!" (Thread.id (Thread.self ())) "dequeue";
            Condition.wait wcond m;
            Printf.printf "%d Got lock again after condition (%s)\n%!" (Thread.id (Thread.self ())) "dequeue";
          done;
          let cbs = !watches in
          watches := [];
          cbs
          ) ()
        in
        Printf.printf "Calling callback...\n%!";
        List.iter (fun cb -> try cb () with _ -> ()) cbs;
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

let process_watch c (path, token) =
  if path = _introduceDomain || path = _releaseDomain then !special_cb (path, token) else begin
  Printf.printf "process_watch: %s %s\n%!" path token;
  with_lock "process watch" (fun () ->
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
        cache := Cache.write t path' v
      | None ->
        cache := Cache.rm t path'
    end) ()
  end

let enable_cache () =
  with_lock "enable cache" (fun () ->
    let c = make_client () in
    client := Some c;
    caching_enabled := true;
    Client.set_watch_callback c (process_watch c);
    Client.immediate c (fun xs ->
      Client.watch xs "/" "tok";
      Client.watch xs _introduceDomain "";
      Client.watch xs _releaseDomain "";
      Client.rm xs "/cache";
      let lslr = ls_lR xs "/" in
      let t = List.fold_left (fun t (p,v) ->
        Cache.write t (list_path p) v) Cache.empty lslr in
      cache := t;
      let _th = WatchQueue.start_dequeue_watches_thread () in
      ()
    )) ()

let get_client () =
  Printf.printf "Get client...\n%!";
  with_lock "get client" (fun () ->
    match !client with
    | None ->
      let c = make_client () in
      client := Some c;
      c
    | Some c ->
      c) ()

let forget_client () = client := None


let counter = ref 0l

module StringSet = Set.Make(struct type t = string let compare = String.compare end)

module PathRecorder = struct
  type t = {
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

  let empty = { accessed_paths = StringSet.empty; watched_paths = StringSet.empty }
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

    better_watch : string list -> string -> (string list -> string -> unit) -> unit;
    better_unwatch : string list -> string -> unit;

    set_special_watch_cb : (string * string -> unit) -> unit;

    (* Compound operations not corresponding one-to-one with those of Client *)
    mkdirperms : string -> Xs_protocol.ACL.t -> unit;
  }

  let sync h =
    let cb _path _tok = with_lock "sync callback" (fun () -> Condition.broadcast wcond) () in
    let path = ["cache"] in 
    let n = with_lock "sync find n" (fun () -> cache_n := Int64.add 1L !cache_n; cache := Cache.watch !cache path "cache" cb; !cache_n) () in
    Client.write h (Printf.sprintf "/%s" (String.concat "/" path)) (Int64.to_string n);
    let check () = match Cache.read !cache path with | Some x -> Printf.printf "cache wait: %s %Ld\n%!" x n; Int64.of_string x < n | None -> true in
    with_lock "sync check cond" (fun () ->
      while check () do
        Printf.printf "%d Releasing lock on condition (%s)\n%!" (Thread.id (Thread.self ())) "sync check cond";
        Condition.wait wcond m;
        Printf.printf "%d Got lock again after condition (%s)\n%!" (Thread.id (Thread.self ())) "sync check cond"
      done
      ) ();
    with_lock "sync remove callback" (fun () -> cache := Cache.unwatch !cache path "cache") ()
    

  let gen_ops path_recorder immediate h =
    let path p =
      PathRecorder.access path_recorder p;
      list_path p in
    if !caching_enabled
    then {
      read =
        (fun p ->
          let t = with_lock "read" (fun () -> !cache) () in
          match Cache.read t (path p) with 
          | Some x -> x
          | None -> raise (Xs_protocol.Enoent p));
      directory =
        (fun p ->
          let t = with_lock "directory" (fun () -> !cache) () in
          match Cache.directory t (path p) with
          | Some x -> x
          | None -> raise (Xs_protocol.Enoent p));
      write =
        (fun p v ->
          ignore(path p);
          Client.write h p v;
          if immediate then sync h);
      writev =
        (fun base_path kvs ->
          List.iter (fun (k, v) ->
            let p = base_path ^ "/" ^ k in 
            ignore(path p);
            Client.write h p v) kvs;
            if immediate then sync h);
      mkdir = 
        (fun p ->
          ignore(path p);
          Client.mkdir h p);
      rm =
        (fun p ->
          ignore(path p);
          (try Client.rm h p with Xs_protocol.Enoent _ -> ());
          if immediate then sync h);
      setperms = Client.setperms h;
      getdomainpath = Client.getdomainpath h;
      watch = (fun _p _tok -> ignore(failwith "Unsupported: use better watches!"));
      unwatch = (fun _p _tok -> ignore(failwith "Unsupported: use better watches!"));
      better_watch = (fun path tok cb -> 
        with_lock "better watch" (fun () ->
          PathRecorder.watch path_recorder (Printf.sprintf "/%s" (String.concat "/" path));
          cache := Cache.watch !cache path tok cb) ());
      better_unwatch = (fun path tok ->
        with_lock "better unwatch" (fun () ->
          PathRecorder.unwatch path_recorder (Printf.sprintf "/%s" (String.concat "/" path));
          cache := Cache.unwatch !cache path tok) ());
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
    better_watch = (fun _ _ _ -> ignore(failwith "Unsupported in uncached xenstore"));
    better_unwatch = (fun _ _ -> ignore(failwith "Unsupported in uncached xenstore"));
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
      counter := Int32.succ !counter;
      let token = Printf.sprintf "%s%ld" "auto_internal:" !counter in
  
      (* We signal the caller via this cancellable task: *)
      let path_recorder = Some PathRecorder.empty in
      let xs = gen_ops path_recorder true (Client.immediate (get_client ()) (fun c -> c)) in
      let unwatch_all () =
        let watched_paths = PathRecorder.watched path_recorder |> StringSet.elements in
        List.iter (fun p -> xs.better_unwatch (list_path p) token) watched_paths
      in

      let t = Xs_client_unix.Task.make () in
      Xs_client_unix.Task.on_cancel t (fun () -> unwatch_all ());

      (* Adjust the paths we're watching (if necessary) and block (if possible) *)
      let rec watch_fn _path _tok =
        try
          let result = f xs in
          Xs_client_unix.Task.wakeup t result;
          unwatch_all ();
        with
          | Xs_protocol.Eagain ->
            adjust_paths ()
          | _ ->
            Xs_client_unix.Task.cancel t
      and adjust_paths () =
        let current_paths = PathRecorder.watched path_recorder in
        Printf.printf "current_paths: [%s]  " (String.concat ";" (StringSet.elements current_paths));
        (* Paths which weren't read don't need to be watched: *)
        Printf.printf "accessed: [%s]  " (String.concat ";" (StringSet.elements (PathRecorder.accessed path_recorder)));
        let old_paths = StringSet.diff current_paths (PathRecorder.accessed path_recorder) in
        Printf.printf "old_paths: [%s]\n%!" (String.concat ";" (StringSet.elements old_paths));
        List.iter (fun p -> xs.better_unwatch (list_path p) token) (StringSet.elements old_paths);
        (* Paths which were read do need to be watched: *)
        let new_paths = StringSet.diff (PathRecorder.accessed path_recorder) current_paths in
        List.iter (fun p -> xs.better_watch (list_path p) token watch_fn) (StringSet.elements new_paths);
      in
      watch_fn [] token;
      t
    end
end

module Xst = Xs
include Xs
