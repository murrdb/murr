# RFC Title

<!--
How to write a murr RFC. Delete all comments before you commit.

Copy this file to NNNN-short-name.md with the next free number. The title
names the thing that changes ("Compound Keys"), without the word "RFC".

Write an RFC when a change is hard to undo, or when somebody will ask "why
is it like this?" a year from now: key and row bytes, the manifest, the
schema, a request or response shape, a dependency that everything sits on.
A bugfix or a refactoring does not need one.

Keep it short. 0001-compound-keys.md has about 200 lines, and that is a
good size. A draft above 400 lines usually holds two RFCs, or a Design
section that repeats the code.

Rules for all sections:

- Write the reasoning. A reader can find a struct with grep. They cannot
  find out why hashed keys lost to length-prefixed keys.
- Use numbers and names. "With 1000 keys per request, parsing that list is
  work we can skip" tells the reader more than "this improves performance".
- Say what you did not do and what you do not know. "The read benchmarks
  compile but have not been run against this change yet" is a fine sentence
  for an RFC. A reader trusts the other sentences more because of it.
- Explain with an example, a table or a small ASCII diagram first. Use Rust
  only for public types and signatures. Keep pseudocode to 7 lines or less.
- Pick one word for one thing and keep it. If it is a "key column" in the
  Summary, it is not a "key field" in the Design.
- Delete a section that has nothing to say. Do not fill it with "N/A".
- Each claim must come from a decision you made, a measurement, or the
  code. This applies most of all to a draft that an LLM wrote: cut all
  sentences that you cannot trace to one of the three.
- When the code drifts from the design, update the RFC and add a line to
  Updates.
-->

Status: Draft

<!-- Draft, Accepted or Implemented. -->

Authors:

* [Your Name](https://github.com/your_github_profile)

## Summary

<!--
Three to six sentences that a user of murr can read with no other context.
Say what changes for them first, and then the one idea that makes it work.
"A table key can now be made of several columns" is a good first sentence.
"This RFC proposes a redesign of the key subsystem" is not.

If the design has a few clear steps, list them here. The reader then knows
the shape before the Design section starts.
-->

## Motivation

<!--
Describe the problem as it is today, with one real example. 0001 has
"feature tables are often keyed by a pair like (user_id, item_id), so users
had to glue the parts into one string on the client". Say who has the
problem and what they do to work around it.

If other systems solved the same problem, link them (RocksDB MultiGet,
Redis MGET). If the argument is about cost, show the cost: a small table of
"today" against "after this RFC" works better than a paragraph.

Do not describe the solution here.
-->

## Goals

<!--
Two to five results that a reviewer can test against the finished code.
"Keys over one or more utf8 and integer columns" can be tested. "A flexible
key model" cannot.
-->

## Non-Goals

<!--
Things that a reader can expect from this RFC and will not get, each with
a short reason: "Range or prefix scans. Key bytes have no meaningful order."
Include the cases where the design is worse than what exists today. Do not
list things that nobody would expect.
-->

## Design

<!--
Start with the user-visible surface: the schema JSON, the request body, the
public type. A reader who stops after the first subsection must know what
the change looks like from outside.

Then use one subsection per decision. In each one, give the rule, then a
concrete example, then the reason. For example: "The length prefix is what
keeps ("ab", "c") and ("a", "bc") apart."

When you remove something, say why it existed and why that reason is gone.

List the edge cases and the rejected inputs as bullets, and say what the
caller gets back, for example "a missing column is a 400".

Leave out everything that a reader gets faster from the code: private
helpers, module layout, error enum variants.
-->

## Compatibility

<!--
Murr is pre-alpha, so a break is allowed. A silent break is not. Go through
the list and write one line for each thing that changes shape:

- data directories: key bytes, row format, manifest.json
- HTTP API and openapi.yaml
- Flight tickets, RPCs and returned schemas
- configuration keys and MURR_ environment variables
- the Python client (shuttie/murr-python)

For each break, say what the user must do, for example "data directories
have to be recreated".
-->

## Performance

<!--
Delete this section if the change is not on the read or write path.

Give the name of the benchmark, the dataset and the batch size, and the
numbers before and after. If you did not run it, say so and say what you
expect and why. Do not write "should be faster" with no number.
-->

## Testing

<!--
Name the cases, not the test types. Write "an rstest table of key shapes:
single int, utf8 + int, two strings that share a boundary". Do not write
"unit tests will be added".

Tests go through the public API of a module. Cover the happy path and the
failures that can really happen.
-->

## Alternatives

<!--
One subsection per alternative that you seriously considered. Include "do
nothing" if it is a real option.

Each one needs three things: what it is in a sentence or two, what is good
about it, and the specific reason it lost. Be fair to it: "Fixed 16-byte
keys are attractive for PlainTable. A collision would return a wrong row
with no error." If you built or measured it, give the number.

A reader who disagrees with the RFC starts here. If they do not find their
idea, they will propose it again.
-->

## Open Questions

<!--
Questions that a reviewer or a benchmark can answer. For each one, say what
changes in the design if the answer goes the other way. A decision that
needs agreement also belongs here: give both sides and say which one the
RFC takes.
-->

## References

<!-- Issues, PRs, papers, docs of other systems. Delete if empty. -->

## Updates

<!--
One dated line per change of the design after the first review: what
changed and why. Delete until there is one.
-->
