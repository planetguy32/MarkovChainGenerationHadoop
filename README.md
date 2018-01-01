Hadoop Markov chain
=====================

MarkovChain.java is a Hadoop program that generates Markov chains,
given text files.

The outputs consist of a plain-text file. First on each line is a string
indicating the first of the pair. Next on the line are tab-separated
pairs of the form "key=p", where "key" is a possible next word and
"p" is the probability of that word observed in the corpus.

MarkovGen.java generates text using Markov chains produced by
MarkovChain.java.
