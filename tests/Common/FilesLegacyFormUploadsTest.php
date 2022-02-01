<?php


namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;

use \Keboola\StorageApi\Options\FileUploadOptions;

class FilesLegacyFormUploadsTest extends StorageApiTestCase
{

    public function testFormUpload()
    {
        $token = $this->_client->verifyToken();
        if (in_array($token['owner']['region'], ['eu-central-1', 'ap-northeast-2'])) {
            $this->markTestSkipped('Form upload is not supported for ' . $token['owner']['region'] . ' region.');
        }
        if (!in_array('legacy-form-uploads', $token['owner']['features'])) {
            $this->markTestSkipped('Form upload is deprecated');
        }

        $path = __DIR__ . '/../_data/files.upload.txt';
        $options = new FileUploadOptions();
        $options->setIsEncrypted(false)
            ->setFileName('neco');

        // using presigned form
        $result = $this->_client->prepareFileUpload($options);
        $uploadParams = $result['uploadParams'];
        $client = new \GuzzleHttp\Client();

        $fh = @fopen($path, 'r');

        $multipart = [
            [
                'name' => 'key',
                'contents' => $uploadParams['key'],
            ],
            [
                'name' => 'acl',
                'contents' => $uploadParams['acl'],
            ],
            [
                'name' => 'signature',
                'contents' => $uploadParams['signature'],
            ],
            [
                'name' => 'policy',
                'contents' => $uploadParams['policy'],
            ],
            [
                'name' => 'AWSAccessKeyId',
                'contents' => $uploadParams['AWSAccessKeyId'],
            ],
            [
                'name' => 'file',
                'contents' => $fh,
            ]
        ];

        $client->post($uploadParams['url'], array(
            'multipart' => $multipart,
        ));

        $file = $this->_client->getFile($result['id']);

        $this->assertEquals($options->getIsPublic(), $file['isPublic']);
        $this->assertEquals('neco', $file['name']);
        $this->assertEquals(file_get_contents($path), file_get_contents($file['url']));
    }

    public function testEncryptionMustBeSetWhenEnabled()
    {
        $token = $this->_client->verifyToken();
        if (in_array($token['owner']['region'], ['eu-central-1', 'ap-northeast-2'])) {
            $this->markTestSkipped('Form upload is not supported in ' . $token['owner']['region'] . ' region.');
        }
        if (!in_array('legacy-form-uploads', $token['owner']['features'])) {
            $this->markTestSkipped('Form upload is deprecated');
        }

        $path = __DIR__ . '/../_data/files.upload.txt';
        $options = new FileUploadOptions();
        $options->setIsEncrypted(true)
            ->setFileName('neco');

        // using presigned form
        $result = $this->_client->prepareFileUpload($options);
        $uploadParams = $result['uploadParams'];
        $this->assertEquals('AES256', $uploadParams['x-amz-server-side-encryption']);
        $client = new \GuzzleHttp\Client();

        $fh = @fopen($path, 'r');

        $multipart = [
            [
                'name' => 'key',
                'contents' => $uploadParams['key'],
            ],
            [
                'name' => 'acl',
                'contents' => $uploadParams['acl'],
            ],
            [
                'name' => 'signature',
                'contents' => $uploadParams['signature'],
            ],
            [
                'name' => 'policy',
                'contents' => $uploadParams['policy'],
            ],
            [
                'name' => 'AWSAccessKeyId',
                'contents' => $uploadParams['AWSAccessKeyId'],
            ],
            [
                'name' => 'file',
                'contents' => $fh,
            ]
        ];

        try {
            $client->post($uploadParams['url'], array(
                'multipart' => $multipart,
            ));
            $this->fail('x-amz-server-side​-encryption should be required');
        } catch (\GuzzleHttp\Exception\ClientException $e) {
            $this->assertEquals(403, $e->getResponse()->getStatusCode());
        }

        // using federation token
        $options = $options->setFederationToken(true);
        $result = $this->_client->prepareFileUpload($options);
        $uploadParams = $result['uploadParams'];
        $this->assertEquals('AES256', $uploadParams['x-amz-server-side-encryption']);

        $s3Client = new \Aws\S3\S3Client([
            'version' => 'latest',
            'region' => $result['region'],
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ],
        ]);

        $putParams = array(
            'Bucket' => $uploadParams['bucket'],
            'Key' => $uploadParams['key'],
            'Body' => fopen($path, 'r+'),
        );

        try {
            $s3Client->putObject($putParams);
            $this->fail('access denied should be thrown');
        } catch (\Aws\S3\Exception\S3Exception $e) {
            $this->assertEquals(403, $e->getStatusCode());
            $this->assertEquals('AccessDenied', $e->getAwsErrorCode());
        }
    }
}
