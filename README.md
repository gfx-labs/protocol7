# protocol7

protocol7 ([insp.](https://lain.fandom.com/wiki/Protocol_7)) is an sdk and set of applications that provide a framework for temporal-orchestrated ai workflows.


it provides the primatives required in order to orchestrate secure and permissioned actions, and most importantly, live agent harness turns, in a permissioned way, all through temporal.

the technologies that are taken advantage of are:

https://spiffe.io/
https://github.com/acp-protocol/acp-spec
https://github.com/gfx-labs/pdp (spiffe and acl aware version of iron-proxy)


this is combined with a postgres database for work/ticket tracking, along with temporal for workflow declaration and state.


you are meant to deploy protocol7 how you wish, and then write your own applications, containers, temporal workers, and code, using said primatives, to create your orchestation loop.

see `docs/` for some assorted examples of how p7 may be used
