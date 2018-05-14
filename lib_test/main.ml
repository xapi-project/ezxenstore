(* A place to put client library tests *)

open Cmdliner

let set_socket_path path = Xs_transport.xenstored_socket := path

let test socket =
  set_socket_path socket;
  let open Xenstore in
  let result = 
    with_xs (fun xs ->
        xs.write "/foo" "bar";
        xs.read "/foo")
  in if result="bar" then `Ok 0 else `Error (false,"argh")

let test_cache socket =
  set_socket_path socket;
  Xenstore.enable_cache ();
  let _ =
    Xenstore.(with_xs (fun xs ->
      Watch.wait_for ~xs (Watch.value_to_become "/foo/bar" "baz");
      Printf.printf "finished\n%!"
    ))
  in
  `Ok 0

let socket =
  let doc = "Set the path to the xenstored socket" in
  Arg.(value & opt string "/var/run/xenstored/socket" & info ["s"] ~doc)

let cmd = 
  let doc = "Test the ezxenstore library" in
  let man = [
    `P "Requires a running xenstored to work. See github.com:mirage/ocaml-xenstore-server"
  ] in
  Term.(ret (const test_cache $ socket)),
  Term.info "test_ezxenstore" ~doc ~man

let () = match Term.eval cmd with `Ok x -> exit x | _ -> exit 1




