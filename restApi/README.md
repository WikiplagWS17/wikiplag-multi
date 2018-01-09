# WikiPlag REST API

provides a REST API to access the plagiarism finder and the database 

## Build and Run locally

```bash
./sbt
>project restApi
> jetty:start
> browse
```

if you want sbt to restart the jetty server on code changes use 
```bash
> ~;jetty:stop;jetty:start
```
instead of 
```bash
> jetty:start
```
## Build Artifact
```bash
sbt "project restApi" clean assembly
```

## Run
```bash
java -jar wiki_rest.jar
```

## Routes

TODO not yet added, see https://github.com/WikiplagWS17/wikiplag

## Good reads
#### Scalatra
* [generate project template](http://scalatra.org/getting-started/first-project.html)

* [deploy standalone](http://scalatra.org/guides/2.4/deployment/standalone.html)
