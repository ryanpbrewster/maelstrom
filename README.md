# What is this?

Working my way through https://fly.io/dist-sys/, a very cool "guided tour" of
distributed systems that is driven by
[maelstrom](https://github.com/jepsen-io/maelstrom).

# My takeaways

The Lamport diagrams (`messages.svg` in the maelstrom output) are extremely
useful to understand the system's behavior. Honestly, abstracting away service
communcation just to get these for unit tests might be worth it.

# Exercises

## Echo

Nothing to it, the Go library is taking care of most of the heavy lifting here.

## Unique ID Generation

Sadly enough, generating random int64s does work here.

I did try to make this a little more principled by splitting up integers by
modulus. Essentially if you have 5 servers, server #0 promises to only assign
numbers that are 0 mod 5, server #1 takes numbers 1 mod 5, etc.

One of the things that tripped me up here was that you don't know the set of
node ids up front. If you invoke the `n.NodeIDs()` function immediately on
startup, it will give you an nil slice. You have to wait until after the `init`
message (which is reliably the first message of the workload).

## Broadcast

Man, what a wild ride this one was. The single-node implementation is basically
trivial, you just append items to a list. But the multi-node implementation was
hard! Gossip protocols seem to have a lot of tradeoffs between efficiency and
propagation speed.

I initially did a naive fanout approach: every time you receive a new broadcast,
immediately fire-and-forget it to all of your neighbors. One of the surprises I
hit there is that you have to register a handle for the `broadcast_ok` _reply_
that your neighbors will send back by default.

Overall, this was pretty inefficient, and it never caught up if a network partition
interrupted one of the messages.

I added an in-memory queue with explicitly required acknowlegement from your
neighbor, and a 100ms retry loop. That did work, but it was very inefficient.

What finally got me to reasonable performance was adding a separate out-of-band
batch gossip RPC, and keeping track of which messages each of our neighbors
know about. I still have a background retry loop in place, and the amount of
message fanout is large. I never actually hit the messages-per-op target
numbers.

## Grow-Only Counter

Honestly, this problem felt kind of weird to me. The underlying KV store being
sequentially-consistent instead of linearizable is harsh.

I worked around it by stuffing a garbage write into every request to basically
force it to provide linearizability. I wasn't clever, I didn't use any kind of
counter sharding to reduce compare-and-swap contention. The request rate didn't
really encourage that.

If you instead use the linearizable KV store, the problem is basically trivial.
And hacking around the lack of linearizability makes me feel like I missed
something.

## Kafka-style log

Moderately complex, but interesting.

I am not sure what the point of the `commit_offsets` RPC is. I understand why
it exists in actual production systems, but AFAICT it's trivial to implement
using the linearizable KV store, and I don't really see how it interacts with
the actual log part of the problem.

My implementation basically hacks in two-phase commit by pre-allocating an
offset using the linearizable KV store, then doing a non-transactional write to
actually persist the record. This could in principle lead to gaps in the offset space.

Additionally, because the KV store doesn't support range scans, we serve `poll` RPCs by doing point
lookups on offsets. If there are large gaps in the offset space, we may have to do a lot of these
in order to bridge the gap. I put in a manual cap of 10.

We could, in principle, do an unbounded scan. We know the largest possible
offset. I just didn't implement that.
