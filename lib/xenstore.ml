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

let with_lock f x =
  Mutex.lock m;
  try
    let result = f x in
    Mutex.unlock m;
    result
  with e ->
    Mutex.unlock m;
    raise e

let watches = ref []

module WatchQueue = struct  
  let enqueue_watch cb =
    (* Nb, this will be called with the mutex already locked *)
    watches := cb :: !watches
end

module Cache = Xscache.Cache(WatchQueue)


let cache = ref Cache.empty
let caching_enabled = ref false
let client = ref None

let strip_leading_slash = function | ""::xs -> xs | x -> x

let rec ls_lR xs dir =
    Printf.printf ".%!";
    let this = try [ dir, Client.read xs dir ] with _ -> [] in
    let subdirs = try Client.directory xs dir |> List.filter (fun x -> x <> "") |> List.map (fun x -> Filename.concat dir x) with _ -> [] in
    this @ (List.concat (List.map (ls_lR xs) subdirs))

let process_watch c (path, _token) =
  with_lock (fun () ->
    let t = !cache in
    let vopt =
      try
        Client.immediate c (fun xs ->
          Some (Client.read xs path))
      with Xs_protocol.Enoent _ -> None
    in
    let path' = Astring.String.cuts ~sep:"/" path |> strip_leading_slash in
    begin
      match vopt with
      | Some v -> 
        cache := Cache.write t path' v
      | None ->
        cache := Cache.rm t path'
    end) ()

let enable_cache () =
  with_lock (fun () ->
    let c = make_client () in
    client := Some c;
    caching_enabled := true;
    Client.set_watch_callback c (process_watch c);
    Client.immediate c (fun xs ->
      Client.watch xs "/" "tok";
      let lslr = ls_lR xs "/" in
      let t = List.fold_left (fun t (p,v) ->
        Cache.write t (Astring.String.cuts ~sep:"/" p |> strip_leading_slash) v) Cache.empty lslr in
      cache := t
    )) ()

let get_client () =
  with_lock (fun () ->
    match !client with
    | None ->
      let c = make_client () in
      client := Some c;
      c
    | Some c ->
      c) ()

let forget_client () = client := None

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

    (* Compound operations not corresponding one-to-one with those of Client *)
    mkdirperms : string -> Xs_protocol.ACL.t -> unit;
  }

  let ops h =
    let path p = Astring.String.cuts ~sep:"/" p |> strip_leading_slash in
    if !caching_enabled
    then begin {
      read =
        (fun p ->
          let t = with_lock (fun () -> !cache) () in
          match Cache.read t (path p) with 
          | Some x -> x
          | None -> raise (Xs_protocol.Enoent p));
      directory =
        (fun p ->
          let t = with_lock (fun () -> !cache) () in
          match Cache.directory t (path p) with
          | Some x -> x
          | None -> raise (Xs_protocol.Enoent p));
      write = Client.write h;
      writev = (fun base_path -> List.iter (fun (k, v) -> Client.write h (base_path ^ "/" ^ k) v));
      mkdir = Client.mkdir h;
      rm = (fun path -> try Client.rm h path with Xs_protocol.Enoent _ -> ());
      setperms = Client.setperms h;
      getdomainpath = Client.getdomainpath h;
      watch = (fun _p _tok -> ignore(failwith "Unsupported: use better watches!"));
      unwatch = (fun _p _tok -> ignore(failwith "Unsupported: use better watches!"));
      better_watch = (fun path tok cb -> with_lock (fun () -> cache := Cache.watch !cache path tok cb) ());
      better_unwatch = (fun path tok -> with_lock (fun () -> cache := Cache.unwatch !cache path tok) ());
      introduce = Client.introduce h;
      set_target = Client.set_target h;
      mkdirperms = (fun path -> (Client.mkdir h path; Client.setperms h path));
    }
    end else {
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
    mkdirperms = (fun path -> (Client.mkdir h path; Client.setperms h path));
    }
  let with_xs f = Client.immediate (get_client ()) (fun h -> f (ops h))
  let wait f = Client.wait (get_client ()) (fun h -> f (ops h))
  let transaction _ f = Client.transaction (get_client ()) (fun h -> f (ops h))

end

module Xst = Xs
include Xs
