# Epistemic Rules

These rules apply when writing documentation, storytelling guides, summary tables, or any text that claims one entity is derived from another.

## Verify Derivation Against Code

Any phrase like "X is derived from Y", "X comes from Y", "X is computed from Y" is a concrete claim about specific lines of code. Before writing it — re-open the function and check the actual generation order.

The risk is highest in summary tables and minimalist diagrams, where 3-step dependency chains get silently compressed into 2-step ones and lose correctness.

If you have not just looked at the code, say so explicitly ("по памяти, надо проверить") rather than asserting confidently.

## Cite Code for Derivation Claims

When asserting that one entity is derived from another, include the `file:line` where the derivation happens. This forces verification before the claim is written and gives the user a place to check.

## Compactness Must Not Cost Correctness

When the user asks for a minimalist or short answer, fewer rows/columns is fine — but each remaining cell must be exact. If precision cannot fit in the requested format, flag the conflict ("в таком формате точно не получится, вот более полная версия") instead of silently dropping fidelity.
