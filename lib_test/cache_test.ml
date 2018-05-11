(** Don't queue watches, immediately execute them *)
module Watchqueue : Xscache.WATCHQUEUE = struct
    let enqueue_watch : Xscache.watch -> unit = fun w -> w ()
end

module C = Xscache.Cache(Watchqueue)

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
    test_watch ()