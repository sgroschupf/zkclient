ZkClient: a zookeeper client, that makes life a little easier. 
=====

+ Website: 			https://github.com/sgroschupf/zkclient
+ Apache 2.0 License

==> see [CHANGELOG][] for recent work


Build ZkClient from sources:
---------------

+ git clone https://github.com/sgroschupf/zkclient.git
+ ./gradlew test _(run the test suite)_
+ ./gradlew jars _(build the jars)_
+ (see available build targets by executing './gradlew tasks' )


Howto release ZkClient as maven artifact
---------------
- sonatype repository is already configured: https://issues.sonatype.org/browse/OSSRH-4783
- generate gpg key and publish it to _hkp://pool.sks-keyservers.net_ (https://docs.sonatype.org/display/Repository/How+To+Generate+PGP+Signatures+With+Maven may be of help)
- tell gradle about the gpg key and sonatype credentials, e.g. through ~/.gradle/gradle.properties: 
  - sonatypeUsername=$yourSonatypeUser
  - sonatypePassword=$yourSonatypePassword
  - signing.keyId=$yourKeyId
  - signing.password=$yourKeyPassphrase
  - signing.secretKeyRingFile=/Users/$username/.gnupg/secring.gpg
- set version in build.gradle to the release version (e.g. 0.5-dev to 0.5) and commit
- update CHANGELOG.markdown
- upload the signed artifacts to the Sonatype repository
  - _gradle clean uploadArchives_  
- go to https://oss.sonatype.org/index.html#stagingRepositories and close the repository 
- check the artifacts and if everything is ok, release the repository (on the same page)
- syncing to central maven repository will then be activated (might take around 2h)
- tag with 
  - _git tag -a $releaseVersion -m "Tag for $releaseVersion release"_
  - _git push --tags_
- set version in build.gradle to the next dev version (e.g 0.5 to 0.6-dev) and commit


[CHANGELOG]: ./CHANGELOG.markdown
