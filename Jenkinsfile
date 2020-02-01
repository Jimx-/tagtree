pipeline {
    agent {
        docker {
            image 'gcc-cmake:latest'
            label 'linux && cpu'
        }
    }

    stages {
        stage('Build') {
            steps {
                sh 'mkdir build'
                dir('build') {
                    sh 'cmake -DCMAKE_BUILD_TYPE=Release -DTAGTREE_USE_AVX2=ON ..'
                    sh 'make -j'
                }
            }

            post {
                cleanup {
                    dir('build') {
                        deleteDir()
                    }
                }
            }
        }
    }
}