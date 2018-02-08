# wikiplag-multi

The Wikiplag project organised as an sbt multi-project project.
This repository containts the "backend" of wikiplag. The webapp is a separate project and can be found [here](https://github.com/WikiplagWS17/webapp).

Wikiplag includes the following suprojects:

* (W) Wikipedia Importer - Cleans up wikipedia articles and creates an inverse index. 
* (P) Plagiarism Finder - The algorithm for detecting potential plagiarisms.
* (U) Utils - Common classes shared between the projects.
* (R) Rest-API: Exposes the plagirism detection algorithm as a REST-API.

Each of the projects has a separate README file with more details in its root directory.

The dependencies of the projects are as follows:
- W -> {U}
- P -> {U}
- U -> {}
- R -> {U,P}

where the "->" arrow denotes the depends on relationship.
