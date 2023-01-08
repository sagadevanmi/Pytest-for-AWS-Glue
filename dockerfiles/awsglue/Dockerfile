FROM amazon/aws-glue-libs:glue_libs_2.0.0_image_01
ENV DISABLE_SSL true
RUN pip3 install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org boto3==1.21.0 moto pg8000 flask==2.1.3 flask_cors==3.0.10 pytest-cov sqlparse
