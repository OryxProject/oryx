title: How To Release
  
This will only be of interest to the project's maintainers!

# Prerequisites

This process requires that you have created a file called, for example, `private-settings.xml` which contains authentication credentials for pushing to the distribution repository. Example:

```
<?xml version="1.0"?>
<settings>
  <servers>
    <server>
      <id>oryx.repo</id>
      <username>snapshots</username>
      <password>...</password>
    </server>
    <server>
      <id>oryx.snapshots.repo</id>
      <username>snapshots</username>
      <password>...</password>
    </server>
  </servers>
</settings>
```

It also requires that you have the GPG key that is written into the project POM, and that you know its passphrase.

# Releasing Maven Artifacts

1. Clone `master` from the repo: `git clone https://github.com/OryxProject/oryx.git`

1. If this is a fresh checkout, optionally configure your user name and email for use with git commits, if not already set globally:
`git config user.name "Your Name"` and `git config user.email "Your Email"`

1. Double-check that tests pass and packaging succeeds first: `mvn clean package`

1. Prepare the release. Consider skipping the (lengthy) tests in these steps with `-DskipTests` if they've been run 
already. To avoid answering the same question many times, the release and new development versions can be 
supplied on the command line:
`mvn -Darguments="-DskipTests" -DreleaseVersion=... -DdevelopmentVersion=... release:prepare`

1. Now perform the release. This will require the `gpg` passphrase for the GPG signing key specified in `pom.xml`:
`mvn -s private-settings.xml -Darguments="-DskipTests -Dgpg.passphrase=..." release:perform`

# Releasing Binaries

1. Download `https://repo1.maven.org/maven2/com/cloudera/oryx/oryx-{batch,speed,serving}/x.y.z/oryx-{batch,speed,serving]-x.y.z.jar`
1. Download `*.sh` from `https://github.com/OryxProject/oryx/tree/oryx-x.y.z/deploy/bin`
1. Navigate to the Github release that was just created, at `https://github.com/OryxProject/oryx/releases/tag/...`
1. Edit the title to something more meaningful like `Oryx x.y.z`
1. Paste brief release notes into the description, including a link to resolved issues for the associated milestone, usually of the form `https://github.com/OryxProject/oryx/issues?q=milestone%3A...+is%3Aclosed`
1. Attach the Batch, Speed, and Serving layer binaries, and scripts, and save the updated release.

# Updating the Site

1. Checkout the release tag if not already: `git checkout -f tags/...`
1. `mvn -DskipTests package`
1. `mvn site`
1. `mvn site:stage site:deploy -pl .`
1. `echo oryx.io > docs/CNAME`
1. `git add docs`
1. `git stash`
1. `git checkout master`
1. `git stash pop`
1. Resolve conflicts in favor of 'theirs' changes
1. `git commit -m "Update site for x.y.z"`
1. `git push origin master`
1. In a minute, check your work at http://oryx.io/
