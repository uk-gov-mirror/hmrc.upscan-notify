import sbt._

object AppDependencies {

  private val bootstrapVersion = "10.7.0"
  private val awsSdkVersion = "2.44.7"

  val compile = Seq(
    "uk.gov.hmrc"            %% "bootstrap-backend-play-30" % bootstrapVersion,
    "org.typelevel"          %% "cats-core"                 % "2.13.0",
    "software.amazon.awssdk" %  "sqs"                       % awsSdkVersion,
    "software.amazon.awssdk" %  "s3"                        % awsSdkVersion,
    "software.amazon.awssdk" %  "secretsmanager"            % awsSdkVersion,
    "jakarta.mail"           %  "jakarta.mail-api"          % "2.1.5",
    "org.eclipse.angus"      %  "jakarta.mail"              % "2.0.5"
  )

  val test = Seq(
    "uk.gov.hmrc"   %% "bootstrap-test-play-30"    % bootstrapVersion % Test
  )

  def apply(): Seq[ModuleID] = compile ++ test
}