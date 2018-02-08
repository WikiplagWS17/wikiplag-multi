# Plagiarism Finder

A algorithm for finding wikipedia plagiarisms.
[Hyperparameters used by the algorithm.](https://github.com/WikiplagWS17/wikiplag-multi/blob/feature/plagiarism-finder-documentation/plagiarismFinder/src/main/scala/de/htwberlin/f4/wikiplag/plagiarism/models/HyperParameters.scala) 

The algorthim works as follows:
* Prepare the user input.
  1. Split into sentences (by splitting on punctuation marks).
  2. Combine short ones. (Minimum length specified by the <code>minimumSentenceLength</code> hyperparmater)
  3. Remove stop words.
  4. Build n-grams.
  5. Compute hashes for each n-gram.

* Find candidate documents.
  1. Query the database (the inverse index table) to get all documents containing the n-grams of the sentences.
  2. For each sentence filter only documents which contain atleast <code>HashesInDocumentThreshold</code> % matching n-gram hashes.

* Find candidate plagiarism sentences in the candidate documents.

  For each sentence in the input text and for each candidate document:
  1. Combine n-grams which have a distance between each other smaller than <code>maxDistanceBetweenNGrams</code> (refered to as candidate sentences).
  2. Filter out candidate sentences where the average distance between the n-grams is greater than <code>maxDistanceBetweenNGrams</code>.
  3. Filter out candidate sentences where the % matching n-gram hashes is lower than <code>HashesInSentenceThreshold</code>. 
  The remaining sentences are the plagiarism candidates in the form of Sentence:[{docId, start position, end position}, ... ,{docId, start position, end position}].
  4. Find the actual end position. Only the starting positions (the position of the first word in the n-gram) of the n-grams are stored in the database. 
  To find the position of the last word in the n-gram one must iterate over the document text starting at the position of the n-gram until n-1 non stop words are encountered.
  This needs to be done for the last n-gram of the candidate sentence to get to the actual end position. 
  An improvement for the future would be to also store this position in the database, alongside the start position of each n-gram.
