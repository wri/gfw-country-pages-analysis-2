FROM lambci/lambda:build-python3.7

WORKDIR /var/task
ENV WORKDIR /var/task
ENV LAMBDANAME gfw-glad-country-pages-update-trigger

# Copy initial source codes into container.
COPY lambda_function.py "$WORKDIR/lambda_function.py"

RUN cd $WORKDIR && \
    zip -9qyr lambda.zip lambda_function.py

CMD aws lambda update-function-code --function-name $LAMBDANAME --zip-file fileb://lambda.zip
#CMD aws lambda create-function --function-name $LAMBDANAME \
#                                --runtime python3.7 \
#                                --role arn:aws:iam::838255262149:role/gfw_lambda_glad_trigger \
#                                --handler lambda_function.lambda_handler \
#                                --zip-file fileb://lambda.zip \
#                                --description "Glad country pages update trigger -> create AWS Batch job" \
#                                --no-publish \
#                                --tags "Project=Global Forest Watch"
#                                --timeout 3

# docker build -t gfw/glad-country-pages-update-trigger .
# docker run --rm -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION  gfw/glad-country-pages-update-trigger