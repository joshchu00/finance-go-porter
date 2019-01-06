node {
  def porterImage

  stage('Git Pull') {
    git url: 'https://github.com/joshchu00/finance-go-porter.git', branch: 'develop'
  }
  stage('Go Build') {
    sh "${tool name: 'go-1.11', type: 'go'}/bin/go build -a -o main"
  }
  stage('Docker Build') {
    docker.withTool('docker-latest') {
      porterImage = docker.build('docker.io/joshchu00/finance-go-porter')
    }
  }
  stage('Docker Push') {
    docker.withTool('docker-latest') {
      docker.withRegistry('', 'DockerHub') {
        porterImage.push()
      }
    }
  }
}
