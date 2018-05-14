(* Xenstore cache *)
type watch = unit -> unit

module type WATCHQUEUE = sig
    val enqueue_watch : watch -> unit
end


module Cache(Q : WATCHQUEUE) = struct  

  type watch_cb = string list -> string -> unit

  type tree = {
    name : string;
    v : string;
    children : tree list;
    watch_cb : (string * watch_cb) list
  }

  type t = {
    tree : tree;
    watches : (string list * ( string * watch_cb)) list;
  }

  let empty = { tree = { name = ""; v = ""; children = []; watch_cb = [] }; watches=[] }


  let rec watch_helper tree path' f =
    match path' with
      | [] -> {tree with watch_cb = f tree.watch_cb}
      | x::xs ->
        if List.exists (fun c -> c.name=x) tree.children
        then begin
          {tree with children = List.map (fun c -> if c.name=x then watch_helper c xs f else c) tree.children}
      end else begin
        tree
      end

  let watch t path tok cb =
    let tree = watch_helper t.tree path (fun watch_cb -> (tok,cb)::watch_cb) in
    Q.enqueue_watch (fun () -> cb path tok); 
    {tree; watches=(path,(tok,cb))::t.watches}

  let unwatch t path tok =
    let tree = watch_helper t.tree path (List.remove_assoc tok) in
    {tree; watches=List.remove_assoc path t.watches}       

  let write (t : t) path value =
    Printf.printf "write path=[%s] value=%s\n%!" (String.concat "/" path) value;
    let rec inner watches tree path' =
      Printf.printf "inner: tree.name=%s path'=[%s] watches = [%s]\n"
        tree.name
        (String.concat "/" path')
        (String.concat ";" 
            (List.map 
                (fun (x,(y,_)) ->
                    Printf.sprintf "{path: [%s]; tok:'%s'}" (String.concat ";" x) y) watches));
      List.iter (fun (tok, cb) -> Q.enqueue_watch (fun () -> cb path tok)) tree.watch_cb;
      match path' with
      | [] ->
        {tree with v=value}
      | x::xs ->
        let (here,further) = List.fold_left (fun (here,further) (watch_path,cb) ->
            match watch_path with
            | [] -> (here,further)
            | [p] when p=x -> (cb::here, further)
            | [_p] -> (here, further)
            | _p::ps -> (here, (ps,cb)::further))
            ([],[])
            watches in
        if List.exists (fun c -> c.name=x) tree.children
        then begin
          let children =
            List.map
              (fun c ->
                 if c.name <> x
                 then c
                 else inner further c xs)
              tree.children in
          {tree with children}
        end else begin
          Printf.printf "Creating new child %s - here=[%s]\n%!" x (String.concat ";" (List.map (fun (tok,_) -> tok) here));
          let child = inner further {name=x; v=""; children=[]; watch_cb=here} xs in 
          {tree with children=child::tree.children}
        end
    in
    {t with tree = inner t.watches t.tree path}

  let read t path =
    let rec inner tree path' =
      match path' with
      | [] -> Some tree.v
      | x::xs ->
        try
            inner (List.find (fun c -> c.name=x) tree.children) xs
        with Not_found ->
            None
    in inner t.tree path

  let directory t path =
    let rec inner tree path' =
      match path' with
      | [] -> Some (List.map (fun c -> c.name) tree.children)
      | x::xs ->
        try
            inner (List.find (fun c -> c.name=x) tree.children) xs
        with Not_found ->
            None
    in inner t.tree path
  

  let rm t path =
    let rec fire_all_watches tree =
        List.iter (fun (tok, cb) -> Q.enqueue_watch (fun () -> cb path tok)) tree.watch_cb;
        List.iter fire_all_watches tree.children
    in       
    let rec inner tree path' =
        List.iter (fun (tok, cb) -> Q.enqueue_watch (fun () -> cb path tok)) tree.watch_cb;
        match path' with
        | [x] ->
            let removed, retained = List.partition (fun c -> c.name = x) tree.children in
            List.iter fire_all_watches removed;   
            {tree with children=retained}
        | x::xs -> {tree with children=List.map (fun c -> if c.name=x then inner c xs else c) tree.children}
        | [] -> failwith "Removing '/' unsupported"
    in
    {t with tree=inner t.tree path}
end


