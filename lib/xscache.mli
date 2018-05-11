type watch = unit -> unit

module type WATCHQUEUE = sig
    val enqueue_watch : watch -> unit
end

module Cache : functor (Q : WATCHQUEUE) -> sig
    type watch_cb = string list -> string -> unit

    type t

    val empty : t

    val watch : t -> string list -> string -> watch_cb -> t

    val unwatch : t -> string list -> string -> t

    val write : t -> string list -> string -> t

    val read : t -> string list -> string option

    val directory : t -> string list -> string list option

    val rm : t -> string list -> t
end