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

1. Check for problems or errors first with `-DdryRun`. Consider skipping the (lengthy) tests in these steps with `-DskipTests` if they've been run already. To avoid answering the same question many times, the release and new development versions can be supplied on the command line:
`mvn -Darguments="-DskipTests" -DdryRun -DreleaseVersion=... -DdevelopmentVersion=... release:prepare`

1. Repeat the above without `-DdryRun` but with `-Dresume=false`.

1. Now perform the release. This will require the `gpg` passphrase for the GPG signing key specified in `pom.xml`:
`mvn -s private-settings.xml -Darguments="-DskipTests -Dgpg.passphrase=..." release:perform`

# Releasing Binaries

1. To get the latest changes and tags post-build, `git pull --tags`
1. Checkout the build tag for this build with `git checkout -f tags/...`
1. `mvn -DskipTests clean package`
1. Assembled binaries appear at `oryx-serving/target/oryx-serving-....jar` and likewise for `speed` and `batch`
1. Navigate to the Github release that was just created, at `https://github.com/OryxProject/oryx/releases/tag/...`
1. Edit the title to something more meaningful like `Oryx x.y.z`
1. Paste brief release notes into the description, including a link to resolved issues for the associated milestone, usually of the form `https://github.com/OryxProject/oryx/issues?q=milestone%3A...+is%3Aclosed`
1. Attach the Batch, Speed, and Serving layer binaries, and scripts in `deploy/bin/`, and save the updated release.

# Updating the Site

1. Using the repo above, checkout the release tag: `git checkout -f tags/...`
1. `mvn clean site:site site:stage`
1. In another location, checkout the site branch into a new directory `oryx-gh-pages`:
`git clone -b gh-pages https://github.com/OryxProject/oryx.git oryx-gh-pages`
You may need to set `user.name` and `user.email` as above if it's a fresh clone.
1. `rm -r .../oryx-gh-pages/*`
1. `cp -r target/staging/* .../oryx-gh-pages/`
1. `cd .../oryx-gh-pages`
1. `git add -A .`
1. `git reset HEAD CNAME`
1. `git checkout -- CNAME`
1. `git commit -m "Update site for ..."`
1. `git push origin gh-pages`
1. In a minute, check your work at http://oryx.io/
1. Optionally delete the repo cloned above if no longer needed.