(** Don't queue watches, immediately execute them *)
module Watchqueue : Xscache.WATCHQUEUE = struct
    let enqueue_watch : Xscache.watch -> unit = fun w -> w ()
end

module C = Xscache.Cache(Watchqueue)

let random_test () =
    let t = ref C.empty in
    let values = Hashtbl.create 1000 in
    let rand_path () =
        let depth = Random.int 6 in
        let rec inner n =
            if n=0 then [] else Printf.sprintf "%x" (Random.int 16) :: inner (n-1)
        in inner depth
    in
    let rec inner n =
        if n=0 then () else begin
            let path = rand_path () in
            let value = Random.int (1024*1024) |> string_of_int in
            t := C.write !t path value;
            Hashtbl.replace values path value;
            inner (n-1)
        end
    in
    inner 1000000;
    Hashtbl.iter (fun k v ->
        match k with
        | "1"::"2"::"3"::_ -> Printf.printf "k=[%s] v=%s\n%!" (String.concat "/" k) v
        | _ -> 
             match C.read !t k with Some v' -> if v' <> v then failwith "Failure!" | None -> failwith "nothing there!") values


let test_watch () =
    let t = C.empty in
    let f_ty = Alcotest.(list (pair (list string) string)) in
    let fired = ref [] in
    let t = C.watch t ["foo"] "tok" (fun path tok -> fired := (path,tok)::!fired) in
    Alcotest.check f_ty "initial fire" [(["foo"],"tok")] !fired;
    fired := [];
    let t = C.write t ["foo"] "hello" in
    Alcotest.check f_ty "fire on write" [(["foo"],"tok")] !fired;
    fired := [];
    let t = C.rm t ["foo"] in
    Alcotest.check f_ty "fire on rm" [(["foo"],"tok")] !fired;
    fired := [];
    let t = C.unwatch t ["foo"] "tok" in
    let t = C.write t ["foo"] "hello" in
    Alcotest.check f_ty "no fire after unwatch" [] !fired;
    fired := [];
    let t = C.watch t ["foo"] "tok" (fun path tok -> fired := (path,tok)::!fired) in
    Alcotest.check f_ty "initial fire" [(["foo"],"tok")] !fired;
    fired := [];
    let t = C.unwatch t ["foo"] "tok" in
    let t = C.rm t ["foo"] in
    Alcotest.check f_ty "no fire on unwatched rm" [] !fired;
    fired := []
    

    
    

let _ =
    test_watch ();
    random_test ()