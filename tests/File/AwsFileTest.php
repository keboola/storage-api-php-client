<?php

namespace Keboola\Test\File;

use GuzzleHttp\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\StorageApiTestCase;

class AwsFileTest extends StorageApiTestCase
{
    /**
     * @dataProvider uploadData
     */
    public function testFileUpload($filePath, FileUploadOptions $options)
    {
        $fileId = $this->_client->uploadFile($filePath, $options);
        $file = $this->_client->getFile($fileId);

        $this->assertEquals($options->getIsPublic(), $file['isPublic']);
        $this->assertEquals($file['isEncrypted'], $options->getIsEncrypted());

        $this->assertEquals(basename($filePath), $file['name']);
        $this->assertEquals(filesize($filePath), $file['sizeBytes']);
        $this->assertEquals(file_get_contents($filePath), file_get_contents($file['url']));

        $tags = $options->getTags();
        sort($tags);
        $fileTags = $file['tags'];
        sort($fileTags);
        $this->assertEquals($tags, $fileTags);

        $info = $this->_client->verifyToken();
        $this->assertEquals($file['creatorToken']['id'], (int) $info['id']);
        $this->assertEquals($file['creatorToken']['description'], $info['description']);

        if ($options->getIsPermanent()) {
            $this->assertNull($file['maxAgeDays']);
        } else {
            $this->assertInternalType('integer', $file['maxAgeDays']);
            $this->assertEquals(StorageApiTestCase::FILE_LONG_TERM_EXPIRATION_IN_DAYS, $file['maxAgeDays']);
        }

        // check attachment, download
        $client = new Client();
        $response = $client->get($file['url']);
        $this->assertStringStartsWith('attachment;', (string) $response->getHeader('Content-Disposition')[0]);
    }

    public function uploadData()
    {
        $path = __DIR__ . '/../_data/files.upload.txt';
        return array(
            array(
                $path,
                (new FileUploadOptions())->setIsPublic(true)
            ),
            array(
                $path,
                (new FileUploadOptions())
                    ->setIsPublic(true)
            ),
            array(
                $path,
                (new FileUploadOptions())
                    ->setIsEncrypted(false)
            ),
            array(
                $path,
                (new FileUploadOptions())
                    ->setIsEncrypted(true)
            ),
            array(
                $path,
                (new FileUploadOptions())
                    ->setNotify(false)
                    ->setCompress(false)
                    ->setIsPublic(false)
            ),
            array(
                $path,
                (new FileUploadOptions())
                    ->setIsPublic(true)
                    ->setIsPermanent(true)
                    ->setTags(array('sapi-import', 'martin'))
            ),
        );
    }

    /**
     * @dataProvider encryptedData
     * @param $encrypted
     */
    public function testFileUploadUsingFederationToken($encrypted)
    {
        $pathToFile = __DIR__ . '/../_data/files.upload.txt';
        $options = new FileUploadOptions();
        $options
            ->setFileName('upload.txt')
            ->setFederationToken(true)
            ->setIsEncrypted($encrypted);

        $result = $this->_client->prepareFileUpload($options);

        $uploadParams = $result['uploadParams'];
        $this->assertArrayHasKey('credentials', $uploadParams);

        $fileId = $this->_client->uploadFile($pathToFile, $options);

        $file = $this->_client->getFile($fileId);

        $this->assertEquals(file_get_contents($pathToFile), file_get_contents($file['url']));

        //all files on Azure are encrypted
        $this->assertEquals($result['isEncrypted'], $options->getIsEncrypted());
        $s3Client = new \Aws\S3\S3Client([
            'version' => 'latest',
            'region' => $result['region'],
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ],
        ]);
        try {
            $s3Client->putObject([                //all files on Azure are encrypted
                'Bucket' => $uploadParams['bucket'],
                'Key' => $uploadParams['key'] . '_part0001',
                'Body' => fopen($pathToFile, 'r+'),
            ]);
            $this->fail('Access denied exception should be thrown');
        } catch (\Aws\S3\Exception\S3Exception $e) {
            $this->assertEquals(403, $e->getStatusCode());
        }
    }

    public function encryptedData()
    {
        return array(
            array(false),
            array(true),
        );
    }

    public function testRequireEncryptionForSliced()
    {
        // sliced file
        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('entries_')
            ->setIsEncrypted(true)
            ->setIsSliced(true);
        $slicedFile = $this->_client->prepareFileUpload($uploadOptions);

        $uploadParams = $slicedFile['uploadParams'];

        $s3Client = new \Aws\S3\S3Client([
            'version' => 'latest',
            'region' => $slicedFile['region'],
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ],
        ]);

        try {
            // write without encryption header
            $s3Client->putObject(array(
                'Bucket' => $uploadParams['bucket'],
                'Key' => $uploadParams['key'] . 'part001.gz',
                'Body' => fopen(__DIR__ . '/../_data/sliced/neco_0000_part_00.gz', 'r+'),
            ))->get('ObjectURL');
            $this->fail('Write should not be allowed');
        } catch (\Aws\S3\Exception\S3Exception $e) {
            $this->assertEquals(403, $e->getStatusCode());
            $this->assertEquals('AccessDenied', $e->getAwsErrorCode());
        }
    }

    public function testGetFileFederationToken()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $fileId = $this->_client->uploadFile($filePath, (new FileUploadOptions())->setNotify(false)->setFederationToken(true)->setIsPublic(false));

        $file = $this->_client->getFile($fileId, (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

        $this->assertArrayHasKey('credentials', $file);
        $this->assertArrayHasKey('s3Path', $file);
        $this->assertArrayHasKey('Expiration', $file['credentials']);

        $s3Client = new \Aws\S3\S3Client([
            'version' => 'latest',
            'region' => $file['region'],
            'credentials' => [
                'key' => $file['credentials']['AccessKeyId'],
                'secret' => $file['credentials']['SecretAccessKey'],
                'token' => $file['credentials']['SessionToken'],
            ],
        ]);

        $object = $s3Client->getObject(array(
            'Bucket' => $file['s3Path']['bucket'],
            'Key' => $file['s3Path']['key'],
        ));
        $this->assertEquals(file_get_contents($filePath), $object['Body']);

        $objects = $s3Client->listObjects(array(
            'Bucket' => $file['s3Path']['bucket'],
            'Prefix' => $file['s3Path']['key'],
        ));

        $this->assertCount(1, $objects->get('Contents'), 'Only one file should be returned');

        try {
            $s3Client->listObjects(array(
                'Bucket' => $file['s3Path']['bucket'],
                'Prefix' => dirname($file['s3Path']['key']),
            ));
            $this->fail('Access denied exception should be thrown');
        } catch (\Aws\S3\Exception\S3Exception $e) {
            $this->assertEquals(403, $e->getStatusCode());
        }

        try {
            $s3Client->listObjects(array(
                'Bucket' => $file['s3Path']['bucket'],
                'Prefix' => $file['s3Path']['key'] . 'manifest',
            ));
            $this->fail('Access denied exception should be thrown');
        } catch (\Aws\S3\Exception\S3Exception $e) {
            $this->assertEquals(403, $e->getStatusCode());
        }
    }
}
