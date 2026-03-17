## ChatGPT doesn't get it right

Almost two years ago, working as a lawyer to understand the requirements of the then-new EU AI Act, I tried an experiment.  I fed the full text of the law (over 140 pages) into ChatGPT and asked whether it covered a particular situation.

It said that it did.  I was fairly sure it did not.

The model pointed me to a section near the end of the law.  That section had nothing to do with the issue.  When I pointed this out, it apologized and cited a nearby section.  That one was also irrelevant.  After several iterations, it continued to insist that the situation was covered, citing sections that were either unrelated or did not exist.

In this case, the error was obvious because I was already familiar with the statute.  But it was easy to see how someone might rely on an answer like this without checking it carefully.  Subsequent legal news has shown that this does happen.

## Why LLMs struggle with long documents

A simple explanation is that these tools hallucinate.  That is not wrong, but it is incomplete.

In my experience, LLMs perform well when working with smaller, focused sets of information.  As the amount of input grows, accuracy drops, often well before the model's formal input limit is reached.  This has been observed elsewhere as well[^1].

The issue is not just length.  It is the model's ability to maintain attention across a large body of text.  With a few pages, performance is often strong.  With hundreds of pages, it becomes unreliable.

## Working through a document more like a lawyer does

This behavior is not entirely different from how people read.  When working through a long document, I retain a structured understanding of what I have read, but I do not memorize the text.  If I need to answer a specific question, I go back to the relevant sections.  Humans process text progressively, retaining summaries as they move through a document rather than holding the full text in mind.  If an AI model can only keep a limited amount of text in focus at one time, it may be dealing with a version of the same constraint.  That observation led to a different approach: instead of asking a model to reason over an entire document at once, step through it in manageable pieces and build up a structured understanding over time.

I began a personal project to implement that idea or discover why it would not work. It turned out that it does work pretty well, and I have now [published](https://github.com/steveyoung-random/cassiel-legal-workbench) the project on GitHub.

The system processes documents in segments and limits what the model sees at any given time.  This keeps the model focused on a narrow context.  When there is not enough information to answer a question, the system records that gap rather than generating an answer.

It uses direct API calls rather than the chat interface that most people are familiar with.  Each call is stateless and includes only the context explicitly provided by the system.  This avoids the accumulation of irrelevant information that can degrade performance.

As the document is processed, the system builds layered summaries at different levels of detail.  These summaries are then used to guide a second stage of analysis, where the model identifies relevant sections and works directly with the substantive text of the original document.  The analysis can expand outward as needed, following references and dependencies.

The goal is to approximate how a lawyer would approach the same task: identify relevant provisions, read them closely, and follow cross-references as needed.

Legal documents are particularly suited to this approach.  Meaning often turns on defined terms, which may be located in one part of a document but apply broadly.  Provisions frequently refer to other provisions.  Regulations may incorporate definitions from statutes or other regulations.

The system maintains a registry of processed documents and tracks definitions, cross-references, and external links.  This allows the model to be given the relevant context for each portion of the analysis, rather than trying to work with the entire document at once.

When given a question by a user, the output is a proposed answer that cites specific provisions.  Those citations are grounded in the source text, not in the model's training data.  The result is not a legal opinion, but a starting point that can be checked and built on.

## Why open source?

I believe that approaches like the one I describe above are likely already used in commercial legal-tech systems.  It's hard to say, though, because companies keep the logic of their systems secret. Because this is a personal project of mine, which I am not trying to monetize, there is no reason for me not to share it widely.

I think that visibility into how AI systems are being used matters for legal work.  Without visibility into the system, it is difficult to assess how such a system handles definitions, cross-references, or context size.  Consider some specific questions a practitioner may not be able to answer about a commercial tool:

- If a term is defined differently in different parts of a document, how is the right definition selected for each provision?
- If a section references another section, how are those linked during analysis?
- How much text is the model seeing at once, and is that within the range it can process reliably?

These are the kinds of questions that determine whether output can be trusted for legal work, and a black-box system makes them hard to answer.

One of the intentions of this project is to make these kinds of details inspectable (and changeable by someone who chooses to).  The system logs the full input and output of every model call.  A log viewer is included, showing the exact prompt and context used at each step.

The AI models themselves remain opaque, but I believe we should do what we can to ensure the surrounding systems that rely on them are not.

The design of this system also prioritizes explicit failure.  If the input has structural issues, processing stops with an error rather than continuing with incomplete results.  For legal analysis, that is important.

## Where to go from here

The current implementation focuses on statutes and regulations.  Their structure makes them a good fit for this approach, and results have been consistent across several bodies of law, including U.S. federal statutes and regulations, California statutes, and EU regulations.  I may extend this to contracts and other types of documents, but that is not an immediate goal.

The code and documentation are available here:  
[https://github.com/steveyoung-random/cassiel-legal-workbench](https://github.com/steveyoung-random/cassiel-legal-workbench)

A more detailed description of the processing pipeline is available here:  
[https://github.com/steveyoung-random/cassiel-legal-workbench/blob/main/docs/PIPELINE_OVERVIEW.md](https://github.com/steveyoung-random/cassiel-legal-workbench/blob/main/docs/PIPELINE_OVERVIEW.md)

[^1]: Cheng-Ping Hsieh, Simeng Sun, Samuel Kriman, Shantanu Acharya, Dima Rekesh, Fei Jia, Yang Zhang, and Boris Ginsburg. (2024). Ruler: What's the real context size of your long-context language models? https://arxiv.org/abs/2404.06654.