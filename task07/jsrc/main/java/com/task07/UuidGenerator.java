package com.task07;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.gson.Gson;
import com.syndicate.deployment.annotations.events.RuleEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import org.joda.time.DateTime;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.syndicate.deployment.model.ResourceType.CLOUDWATCH_RULE;

@LambdaHandler(lambdaName = "uuid_generator",
	roleName = "uuid_generator-role"
)
@RuleEventSource(targetRule = "uuid_trigger")
@DependsOn(name = "uuid_trigger", resourceType = CLOUDWATCH_RULE)
public class UuidGenerator implements RequestHandler<Object, Void>
{
	private static final String AWS_REGION = "eu-central-1";
	private static final int ITERATIONS = 10;
	private static final String BUCKET_NAME = "cmtr-52e956b4-uuid-storage-test";
	public static final String FILE_UPLOADED_MESSAGE = "List of %d UUIDs was added to file %s and uploaded into %s bucket";

	private AmazonS3 amazonS3;
	private LambdaLogger logger;

	public Void handleRequest(Object request, Context context) {
		this.initS3Client();
		this.initLambdaLogger(context);

		Gson gson = new Gson();
		String fileName = getFileName();
		List<String> UUIDs = Stream.generate(this::generateUUID)
				.limit(ITERATIONS)
				.collect(Collectors.toList());

		amazonS3.putObject(BUCKET_NAME, fileName, gson.toJson(UUIDs));
		logger.log(String.format(FILE_UPLOADED_MESSAGE, ITERATIONS, fileName, BUCKET_NAME));

		return null;
	}

	private String generateUUID()
	{
		return UUID.randomUUID().toString();
	}

	private String getFileName()
	{
		final DateTimeFormatter formatter =
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

		return LocalDateTime.now().format(formatter);
	}

	private void initLambdaLogger(final Context context)
	{
		this.logger = context.getLogger();
	}

	private void initS3Client()
	{
		this.amazonS3 = AmazonS3ClientBuilder.standard()
				.withRegion(AWS_REGION)
				.build();
	}
}
