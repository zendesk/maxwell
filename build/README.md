# Releasing a new version to the Maven
The release script can be run locally to publish a new Maxell version to Maven, as long as you have everything set up.

## Prerequisites
Have the following installed and setup on your local machine:

- java 8
- ruby (The same Ruby version as this poject)
- bundler
- [Maven](https://github.com/rajivkanaujia/alphaworks/wiki/Installing-Maven)
  - `brew install maven`
- [MKDocs](https://www.mkdocs.org)
- [gpg](#setup-gpg)

### Setup GPG
To install GPG, run the following in terminal
```shell
`brew install gpg`
```

Then generate gpg key:
```shell
gpg --full-generate-key
``` 

note: 
- if you set a passphrase for your gpg key then you will need to remember it and put it into the `~/.m2/settings.xml` below.
- when the prompt appears choose the following:
  - key type: accept Default
  - key size: accept Default
  - key expiry: 2 years

## Setup Credentials

- [OSSRH Credentials](#setup-ossrh-credentials)
- [Github Token](#setup-github-token)

### Setup OSSRH Credentials
Set the ossrh credentials for the opensource maven repo in the `settings.xml` file. Ask Goanna for credentials if you don't have it.

Example: `~/.m2/settings.xml`
```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                          https://maven.apache.org/xsd/settings-1.0.0.xsd">
    <localRepository/>
      <interactiveMode/>
      <offline/>
      <pluginGroups/>
    <servers>
    <server>
      <id>ossrh</id>
      <username>zendesk_maxwell</username>
      <password>ossrh_password_for_zendesk_maxwell</password>
    </server>
    </servers>
    <mirrors/>
    <proxies/>
    <profiles>
        <profile>
        <id>ossrh</id>
        <activation>
            <activeByDefault>true</activeByDefault>
        </activation>
        <properties>
            <gpg.executable>gpg</gpg.executable>
            <gpg.passphrase>gpg_key_passphrase</gpg.passphrase>
        </properties>
        </profile>
    </profiles>
    <activeProfiles/>
</settings>

```

### Setup Github Token
Create Github token and put it into the `~/.netrc` file. 

Ensure you have write access to this file:
```shell
chmod 0600 ~/.netrc
```

Example: `~/.netrc file`
```
machine api.github.com
    login github-username
    password github-token
```

### Setup GPG
If you have a passphrase on your gpg keys, put the following into your ~/.profile so that the gpg-agent knows the right tty to use.

```
export GPG_TTY=$(tty)
```

Run the following command to list your gpg keys
```
gpg --list-keys
```

run the following to send your public gpg key to the server.
This will take a little time to replicate, so maybe go grab a snack.
```
gpg --send-key <public-gpg-key>
```

## Releasing a version
To release a new version simply cd into the build folder and run the release script. This script will automatically bump up the version, tag and release to maven. 

```shell
cd ./build
bundle exec ./release <the-new-version-number>
```

Note: this script is not idempotent so if any errors occure, you will need to run each step manually. The release script takes the step as an option.

```shell
bundle exec ./release <the-new-version-number> <build | tag | release | changelog | docs | nexus>
```

Check the Maven Repo to see if it all worked!

```
https://repo1.maven.org/maven2/com/zendesk/maxwell/<the-new-version-number>/
```
