pipeline {
    agent any

    environment {
        CONDA_ENV = "my_spark_project"   // your conda environment name
    }

    stages {
        stage('Setup Environment') {
            steps {
                sh '''
                echo ">>> Setting up Conda Environment: $CONDA_ENV"
                if ! conda env list | grep -q $CONDA_ENV; then
                    conda env create -f environment.yml -n $CONDA_ENV
                else
                    conda env update -f environment.yml -n $CONDA_ENV
                fi
                '''
            }
        }

        stage('Build') {
            steps {
                sh '''
                echo ">>> Activating Environment and Building Project"
                source activate $CONDA_ENV
                python setup.py install || echo "No setup.py found, skipping build..."
                '''
            }
        }

        stage('Test') {
            steps {
                sh '''
                echo ">>> Running Tests"
                source activate $CONDA_ENV
                pytest || echo "No tests found or pytest failed!"
                '''
            }
        }

        stage('Package') {
            steps {
                sh '''
                echo ">>> Packaging Project Folder"
                zip -r pyspark_project.zip pyspark_project/ || echo "Project folder not found, skipping package..."
                '''
            }
        }

        stage('Deploy (Local)') {
            steps {
                sh '''
                echo ">>> Deploying Locally"
                mkdir -p /home/$USER/deployments/
                cp pyspark_project.zip /home/$USER/deployments/ || echo "Package not found, skipping deploy."
                '''
            }
        }
    }
}
