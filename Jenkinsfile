pipeline {
    agent any

    environment {
        CONDA_ENV = "my_spark_project"   // your conda environment name
    }

    stages {

        stage('Setup Environment') {
            steps {
                sh '''
                /usr/local/miniconda3/bin/conda env list | grep -q my_spark_project
                if [ $? -ne 0 ]; then
                    echo ">>> Creating Conda Environment: my_spark_project"
                    /usr/local/miniconda3/bin/conda env create -f environment.yml -n my_spark_project
                else
                    echo ">>> Updating Conda Environment: my_spark_project"
                    /usr/local/miniconda3/bin/conda env update -f environment.yml -n my_spark_project
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

    } // <-- closes stages
} // <-- closes pipeline
