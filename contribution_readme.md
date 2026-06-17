# Contribution 1: "feat: Support nested replication keys"

**Contribution Number:** 1  
**Student:** Daria Hrabar  
**Issue:** https://github.com/meltano/sdk/issues/1198  
**Status:** Phase II Complete

---

## Why I Chose This Issue

This issue entails implementing support for nested replication keys to track incremental data updates. By enabling the SDK to parse deep JSON paths directly, this update optimizes data pipelines and removes the need for manual data restructuring.

As a developing software engineer, I chose this issue to learn best practices for contributing to open-source projects in a welcoming environment. I am also excited to expand my knowledge of Python, JSON, API, and AI. Lastly, I hope to make a positive impact on the GitHub community by offering a solution that can be integrated into the main codebase.

---

## Understanding the Issue

### Problem Description

[In your own words, what's broken or missing?]

### Expected Behavior

[What should happen?]

### Current Behavior

[What actually happens?]

### Affected Components

[Which parts of the codebase are involved?]

---

## Reproduction Process

### Environment Setup

*Step 1:* Install project prerequisites on your local device. More details can be found on [the Prerequisites page](https://docs.meltano.com/contribute/prerequisites/) within the Meltano Documentation.
  1. [Python 3.10+](https://www.python.org/downloads/)
  2. [uv](https://docs.astral.sh/uv/)
  3. [Node 18+](https://nodejs.org/)
  4. [Yarn](https://yarnpkg.com/)
  5. Although not listed as an official prerequisite, it is also recommended to install [Visual Studio Build Tools for C++](https://visualstudio.microsoft.com/visual-cpp-build-tools/) to avoid the ModuleNotFoundError.

*Step 2:* Complete the setup of your local development environment.
  1. Clone the forked repository to your local device by either:
     - Running `git clone git@github.com:[your/github/file/path].git` and `cd sdk` in your terminal, or
     - Completing the process directly through GitHub Desktop.
  2. Install Nox and pre-commit by running `uv tool install nox` and `uv tool install pre-commit`.
  3. If not already there, navigate to the cloned local repository and install dependencies by running `uv sync`.
  4. Install pre-commit hooks by running `pre-commit install --install-hooks`. 

When working in VS Code, the virtual environment should become activated automatically. If not, run the `.venv\Scripts\Activate.ps1` command in your terminal. Your .venv is activated if you can see (singer-sdk) displayed at the beginning of your file path in the terminal window.

### Steps to Reproduce

*Setting up the issue reproduction:*

1. In the root of your local sdk folder, create a new Python file called reproducing_issue_1198.py.
2. In the file, define a stream with a nested schema, meaning one field `attributes` contains sub-fields `created` and `updated` inside it, rather than all fields sitting at the top level.
3. Set the stream's `replication_key` to `"attributes.updated"`—a dotted path pointing to the nested timestamp field.
4. Add at least two fake records where the updated timestamp is stored inside attributes, not at the top level of the record.
5. Add a print statement that shows what value the SDK resolves when it looks up the replication key against each record.

*Running the reproduction*

1. Confirm you are inside the sdk folder before running terminal commands.
2. Confirm the virtual environment is active — your prompt should start with (singer-sdk).
3. Run the file with `python reproducing_issue_1198.py`.
4. Observe the output for both records in the terminal.

*Confirming the bug*

1. Check that the printed value for `replication_key` shows `"attributes.updated"`—the dotted path you set.
2. Check that the printed SDK resolved value shows `None` for both records—this is the bug. The SDK cannot find the nested field and returns nothing instead of the expected timestamp.

*Verifying the issue is consistent*

1. Run `python reproducing_issue_1198.py` a second time without changing anything.
2. Confirm the output is identical — both records return `None` again, proving the failure is consistent and not random.

### Reproduction Evidence

- **Commit showing reproduction:** [Link to commit in your fork]
- **Screenshots/logs:** [If applicable]
- **My findings:** [What you discovered during reproduction]

---

## Solution Approach

### Analysis

[Your analysis of the root cause - what's causing the issue?]

### Proposed Solution

[High-level description of your fix approach]

### Implementation Plan

Using UMPIRE framework (adapted):

**Understand:** [Restate the problem]

**Match:** [What similar patterns/solutions exist in the codebase?]

**Plan:** [Step-by-step implementation plan]
1. [Modify file X to do Y]
2. [Add function Z]
3. [Update tests]

**Implement:** [Link to your branch/commits as you work]

**Review:** [Self-review checklist - does it follow the project's contribution guidelines?]

**Evaluate:** [How will you verify it works?]

---

## Testing Strategy

### Unit Tests

- [ ] Test case 1: [Description]
- [ ] Test case 2: [Description]
- [ ] Test case 3: [Description]

### Integration Tests

- [ ] Integration scenario 1
- [ ] Integration scenario 2

### Manual Testing

[What you tested manually and results]

---

## Implementation Notes

### Week [X] Progress

[What you built this week, challenges faced, decisions made]

### Week [Y] Progress

[Continue documenting as you work]

### Code Changes

- **Files modified:** [List]
- **Key commits:** [Links to important commits]
- **Approach decisions:** [Why you chose certain approaches]

---

## Pull Request

**PR Link:** [GitHub PR URL when submitted]

**PR Description:** [Draft or final PR description - much of the content above can be adapted]

**Maintainer Feedback:**
- [Date]: [Summary of feedback received]
- [Date]: [How you addressed it]

**Status:** [Awaiting review / Iterating / Approved / Merged]

---

## Learnings & Reflections

### Technical Skills Gained

[What you learned technically]

### Challenges Overcome

[What was hard and how you solved it]

### What I'd Do Differently Next Time

[Reflection on your process]

---

## Resources Used

- [Link to helpful documentation]
- [Tutorial or Stack Overflow post that helped]
- [GitHub issues or discussions that helped]
