# Use Generative AI With  Amazon Bedrock To Create Live Sports Commentary
Recent breakthroughs in foundation models have accelerated AI innovations to new heights. A new category of AI called Generative AI, refers to a category of AI that generate new outputs based on the data they have been trained on. This class of models has shown promising results in generating creative contents. For example, new audio, code, images, text, simulations, and videos.

In this project, we are going to look into how Generative AI can be used to create highly personalized contents for sport fans. In our example, we'll use a foundation model to generate play by play  commentary based on American Football game data synthetically created (in reality, the data could be directly sourced from the stadiums, or cloud). We'll instruct the model to generate variety of commentary using different prompts. For instance, create prompts to the model to generate commentary in particular commentary writing style, or a particular language that the fans would prefer.  

# Code Examples
All code samples that demonstrate the capabilities described above are provided in this repository. The foundation model used throughout the project is [AI21 Jurassic-2 Ultra](https://aws.amazon.com/bedrock/jurassic/) which is available through [Amazon Bedrock](https://aws.amazon.com/bedrock). This project also comes with a demo UI that demontrates the model ability to generate commentaries base on the style and language. It could be extended to provide many other capablitiies. 

# Dataset
The dataset used for this project is synthetically generated and randomized to provide a format and terminologies based on American Football. 

# Prerequisites
To demonstrate the GenAI capability, we've provided a jupyter notebook with prompts and data processing steps. This notebook has been tested in SageMaker Notebook instance and SageMaker Studio environments. However, it could easily be adopted outside of these Sageaker IDE environments.
To run the notebook and the demo application successfully, you need access to call SageMaker APIs. In particular, IAM permissions to access and to deploy models to SageMaker endpoint. Follow this [documentation](https://docs.aws.amazon.com/sagemaker/latest/dg/security_iam_service-with-iam.html) for setting up appropriate IAM Role and Policies in SageMaker.

# Quick Start For Demo Environment
A demo UI is created that allows users to control how sports commentary are generated using the [Jurassic-2 Ultra](https://aws.amazon.com/bedrock/jurassic/) in [Amazon Bedrock](https://aws.amazon.com/bedrock/). All integrations between the application components and FM model is done using Amazon Bedrock API calls. 

## AWS Lambda Function
This demo relies on an lambda function to orchestrate the interaction between messages from Kinesis and Amazon Bedrock Jurassic-2 Ultra model. A script ```build_and_deploy_lambda.py``` is provided to compile and deploy the lambda function in the AWS account.

## AWS Kinesis Data Stream
In addition to AWS lambda, the telemetry data is simulated and streamed into the application via Kinesis Data Stream. Specifically, 1 data stream (e.g. sports-data-live-stream-src) for data ingestion, and 1 data stream (e.g. sports-data-live-commentaries) for publishing the generated commentary. The lambda function above is designed to take an environment variable to identify the target Kinesis stream (e.g. sports-data-live-commentaries) so it could be consumed by the application. The source Kinesis stream should be configured as a trigger in the lambda function.

```
  git clone https://github.com/wei-m-teh/sagemaker-genai-sports-commentary
  cd sagemaker-genai-sports-commentary
  pip install -r requirements.txt
  export AWS_ACCESS_KEY_ID=<your AWS creds>
  export AWS_SECRET_ACCESS_KEY=<your AWS creds>
  export AWS_SESSION_TOKEN=<your AWS session token> #Only needed if using IAM assumed role
  export AWS_DEFAULT_REGION=<your region>
  python app.py &

```

Open a browser tab with URL: http://localhost:7860

Here's a screenshot of the UI in action:
 
<img src="img/genai-sports-commentary-demo.gif" width="1000" height="500" />


# Security
See [CONTRIBUTING](CONTRIBUTING.md) for more information.

# License
This library is licensed under the MIT-0 License. See the LICENSE file.
