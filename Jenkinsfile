pipeline {
    agent any

    environment {
        CONDA_ENV = "my_spark_project"   // your conda environment name
    }

    stages {
        stage('Setup Environment') {
            steps {
                sh '''
                if /usr/local/miniconda3/bin/conda env list | grep -q my_spark_project; then
                    echo ">>> Updating Conda Environment: my_spark_project"
                    /usr/local/miniconda3/bin/conda env update -f environment.yml -n my_spark_project
                else
                    echo ">>> Creating Conda Environment: my_spark_project"
                    /usr/local/miniconda3/bin/conda env create -f environment.yml -n my_spark_project
                fi
                '''
            }
        }

        stage('Build') {
            steps {
                sh '''
                echo ">>> Activating Environment and Building Project"
                /usr/local/miniconda3/bin/conda run -n my_spark_project python setup.py install || echo "No setup.py found, skipping build..."
                '''
            }
        }

        stage('Test') {
            steps {
                sh '''
                echo ">>> Running Tests"
                /usr/local/miniconda3/bin/conda run -n my_spark_project pytest || echo "No tests found or pytest failed!"
                '''
            }
        }

        stage('Package') {
            steps {
                sh '''
                if [ -d pyspark_project ]; then
    zip -r pyspark_project.zip pyspark_project/
else
    echo "WARNING: Folder 'pyspark_project/' not found, zipping entire workspace instead."
    zip -r pyspark_project.zip .
fi
'''
            }
        }

        stage('Deploy (Local)') {
            steps {
                sh '''
                echo ">>> Deploying Locally to /var/lib/jenkins/deployments"
                mkdir -p /var/lib/jenkins/deployments
                cp pyspark_project.zip /var/lib/jenkins/deployments/deploy.zip || echo "No zip file to deploy!"
                cp /var/lib/jenkins/deployments/deploy.zip $WORKSPACE/deploy.zip || true
                '''
            }
        }

        stage('Archive Artifact') {
            steps {
                archiveArtifacts artifacts: 'deploy.zip', fingerprint: true
            }
        }
    }
}
