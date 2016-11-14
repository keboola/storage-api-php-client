<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;

use \Keboola\StorageApi\Options\FileUploadOptions;
use \Keboola\StorageApi\Options\ListFilesOptions;

class FilesTest extends StorageApiTestCase
{

    public function testFileList()
    {
        $options = new FileUploadOptions();
        $fileId = $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', $options);
        $files = $this->_client->listFiles(new ListFilesOptions());
        $this->assertNotEmpty($files);

        $uploadedFile = reset($files);
        $this->assertEquals($fileId, $uploadedFile['id']);
        $this->assertArrayHasKey('region', $uploadedFile);
    }

    public function testFilesListFilterByTags()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';

        $this->createAndWaitForFile($filePath, new FileUploadOptions());
        $tag = uniqid('tag-test');
        $fileId = $this->createAndWaitForFile($filePath, (new FileUploadOptions())->setTags(array($tag)));

        $files = $this->_client->listFiles((new ListFilesOptions())->setTags(array($tag)));

        $this->assertCount(1, $files);
        $file = reset($files);
        $this->assertEquals($fileId, $file['id']);

        $tag2 = uniqid('tag-test-2');
        $fileId2 = $this->createAndWaitForFile($filePath, (new FileUploadOptions())->setTags(array($tag, $tag2)));

        $files = $this->_client->listFiles((new ListFilesOptions())->setTags(array($tag, $tag2)));
        $this->assertCount(2, $files, 'files with one or more matching tags are returned');
        $file2 = array_shift($files);
        $file = array_shift($files);
        $this->assertEquals($fileId2, $file2['id']);
        $this->assertEquals($fileId, $file['id']);
    }

    public function testFilesListFilterByInvalidValues()
    {
        try {
            $this->_client->apiGet('storage/files?' . http_build_query([
                    'tags' => 'tag',
                ]));
            $this->fail('Validation error should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.validation', $e->getStringCode());
        }
    }

    public function testSetTagsFromArrayWithGaps()
    {
        $file = $this->_client->prepareFileUpload((new FileUploadOptions())
            ->setFileName('test.json')
            ->setFederationToken(true)
            ->setTags([
                0 => 'neco',
                12 => 'another',
            ]));
        $this->assertEquals(['neco', 'another'], $file['tags']);
    }

    public function testFileListSearch()
    {

        $fileId = $this->_client->uploadFile(__DIR__ . '/../_data/users.csv', new FileUploadOptions());
        $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', new FileUploadOptions());

        $files = $this->_client->listFiles((new ListFilesOptions())->setQuery('users')->setLimit(1));

        $this->assertCount(1, $files);
        $file = reset($files);
        $this->assertEquals($fileId, $file['id']);
    }

    public function testSyntaxErrorInQueryShouldReturnUserError()
    {
        try {
            $this->_client->listFiles((new ListFilesOptions())->setQuery('tags[]:sd'));
            $this->fail('exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('query.syntax', $e->getStringCode());
        }
    }

    public function testFileListFilterBySinceIdMaxId()
    {
        $files = $this->_client->listFiles((new ListFilesOptions())
            ->setLimit(1)
            ->setOffset(0));

        $lastFile = reset($files);
        $lastFileId = $lastFile['id'];

        $firstFileId = $this->createAndWaitForFile(__DIR__ . '/../_data/users.csv', new FileUploadOptions());
        $secondFileId = $this->createAndWaitForFile(__DIR__ . '/../_data/users.csv', new FileUploadOptions());

        $files = $this->_client->listFiles((new ListFilesOptions())->setSinceId($lastFileId));
        $this->assertCount(2, $files);

        $this->assertEquals($firstFileId, $files[1]['id']);
        $this->assertEquals($secondFileId, $files[0]['id']);

        $files = $this->_client->listFiles((new ListFilesOptions())->setMaxId($secondFileId)->setLimit(1));
        $this->assertCount(1, $files);
        $this->assertEquals($firstFileId, $files[0]['id']);
    }

    public function testFileListFilterByRunId()
    {
        $options = new FileUploadOptions();
        $options->setFileName('upload.txt')
            ->setFederationToken(true);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $fileId = $this->createAndWaitForFile(__DIR__ . '/../_data/users.csv', $options);
        $file = $this->_client->getFile($fileId);
        $this->assertEquals($runId, $file['runId']);

        $files = $this->_client->listFiles((new ListFilesOptions())->setRunId($runId));

        $this->assertCount(1, $files);
        $this->assertEquals($file['id'], $files[0]['id']);
    }

    /**
     * @dataProvider uploadData
     */
    public function testFileUpload($filePath, FileUploadOptions $options)
    {
        $fileId = $this->_client->uploadFile($filePath, $options);
        $file = $this->_client->getFile($fileId);

        $this->assertEquals($options->getIsPublic(), $file['isPublic']);
        $this->assertEquals(basename($filePath), $file['name']);
        $this->assertEquals(filesize($filePath), $file['sizeBytes']);
        $this->assertEquals(file_get_contents($filePath), file_get_contents($file['url']));

        $tags = $options->getTags();
        sort($tags);
        $fileTags = $file['tags'];
        sort($fileTags);
        $this->assertEquals($tags, $fileTags);

        $info = $this->_client->verifyToken();
        $this->assertEquals($file['creatorToken']['id'], (int)$info['id']);
        $this->assertEquals($file['creatorToken']['description'], $info['description']);
        $this->assertEquals($file['isEncrypted'], $options->getIsEncrypted());

        if ($options->getIsPermanent()) {
            $this->assertNull($file['maxAgeDays']);
        } else {
            $this->assertInternalType('integer', $file['maxAgeDays']);
            $this->assertEquals(180, $file['maxAgeDays']);
        }
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
            'Body' => fopen($pathToFile, 'r+'),
        );

        if ($options->getIsEncrypted()) {
            $putParams['ServerSideEncryption'] = $uploadParams['x-amz-server-side-encryption'];
        }

        $s3Client->putObject($putParams);

        $file = $this->_client->getFile($result['id']);

        $this->assertEquals(file_get_contents($pathToFile), file_get_contents($file['url']));
        $this->assertEquals($result['isEncrypted'], $options->getIsEncrypted());

        try {
            $s3Client->putObject(array(
                'Bucket' => $uploadParams['bucket'],
                'Key' => $uploadParams['key'] . '_part0001',
                'Body' => fopen($pathToFile, 'r+'),
            ));
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

    public function testFormUpload()
    {
        $token = $this->_client->verifyToken();
        if (in_array($token['owner']['region'], ['eu-central-1', 'ap-northeast-2'])) {
            $this->markTestSkipped('Form upload is not supported for ' . $token['owner']['region'] . ' region.');
            return;
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
            return;
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

    public function testSlicedFileUpload()
    {
        $pathToFile = __DIR__ . '/../_data/files.upload.txt';
        $options = new FileUploadOptions();
        $options
            ->setIsSliced(true)
            ->setIsEncrypted(false)
            ->setFileName('upload.txt');

        $preparedFile = $this->_client->prepareFileUpload($options);

        $uploadParams = $preparedFile['uploadParams'];
        $this->assertArrayHasKey('credentials', $uploadParams);
        $this->assertTrue($preparedFile['isSliced']);


        $s3Client = new \Aws\S3\S3Client([
            'version' => 'latest',
            'region' => $preparedFile['region'],
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ],
        ]);

        $part1URL = $s3Client->putObject(array(
            'Bucket' => $uploadParams['bucket'],
            'Key' => $uploadParams['key'] . 'part001',
            'Body' => fopen($pathToFile, 'r+'),
        ))->get('ObjectURL');
        $part2URL = $s3Client->putObject(array(
            'Bucket' => $uploadParams['bucket'],
            'Key' => $uploadParams['key'] . 'part002',
            'Body' => fopen($pathToFile, 'r+'),
        ))->get('ObjectURL');

        $manifest = array(
            'entries' => array(
                array(
                    'url' => $part1URL,
                ),
                array(
                    'url' => $part2URL,
                )
            ),
        );
        $s3Client->putObject(array(
            'Bucket' => $uploadParams['bucket'],
            'Key' => $uploadParams['key'] . 'manifest',
            'Body' => json_encode($manifest),
        ));

        $file = $this->_client->getFile($preparedFile['id']);
        $this->assertEquals(json_encode($manifest), file_get_contents($file['url']));

        // download sliced file
        $file = $this->_client->getFile($preparedFile['id'], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));
        $this->assertTrue($file['isSliced']);

        $s3Client = new \Aws\S3\S3Client([
            'version' => 'latest',
            'region' => $file['region'],
            'credentials' => [
                'key' => $file['credentials']['AccessKeyId'],
                'secret' => $file['credentials']['SecretAccessKey'],
                'token' => $file['credentials']['SessionToken'],
            ],
        ]);

        $objects = $s3Client->listObjects(array(
            'Bucket' => $file['s3Path']['bucket'],
            'Prefix' => $file['s3Path']['key'],
        ));

        $this->assertCount(3, $objects->get('Contents'));

        $object = $s3Client->getObject(array(
            'Bucket' => $file['s3Path']['bucket'],
            'Key' => $file['s3Path']['key'] . 'manifest',
        ));
        $this->assertEquals(json_encode($manifest), $object['Body']);

        $object = $s3Client->getObject(array(
            'Bucket' => $file['s3Path']['bucket'],
            'Key' => $file['s3Path']['key'] . 'part001',
        ));
        $this->assertEquals(file_get_contents($pathToFile), $object['Body']);

        $object = $s3Client->getObject(array(
            'Bucket' => $file['s3Path']['bucket'],
            'Key' => $file['s3Path']['key'] . 'part002',
        ));
        $this->assertEquals(file_get_contents($pathToFile), $object['Body']);
    }


    /**
     * @dataProvider uploadData with compress = true
     */
    public function testFileUploadCompress()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $fileId = $this->_client->uploadFile($filePath, (new FileUploadOptions())->setCompress(true));
        $file = $this->_client->getFile($fileId);

        $this->assertEquals(basename($filePath) . ".gz", $file['name']);

        $gzFile = gzopen($file['url'], "r");
        $this->assertEquals(file_get_contents($filePath), gzread($gzFile, 524288));
    }

    public function testFileDelete()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        ;
        $options = new FileUploadOptions();

        $fileId = $this->_client->uploadFile($filePath, $options);
        $this->_client->getFile($fileId);

        $this->_client->deleteFile($fileId);

        try {
            $this->_client->getFile($fileId);
            $this->fail('File should not exists');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.files.notFound', $e->getStringCode());
        }
    }

    public function testNotExistingFileUpload()
    {
        try {
            $this->_client->uploadFile('invalid.csv', new FileUploadOptions());
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('fileNotReadable', $e->getStringCode());
        }
    }

    public function uploadData()
    {
        $path = __DIR__ . '/../_data/files.upload.txt';
        ;
        return array(
            array(
                $path,
                (new FileUploadOptions())->setIsPublic(true)
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


    public function testFilesPermissions()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $uploadOptions = new FileUploadOptions();

        $newTokenId = $this->_client->createToken(array(), 'Files test');
        $newToken = $this->_client->getToken($newTokenId);
        $firstFileId = $this->createAndWaitForFile($filePath, $uploadOptions);

        $totalFilesCount = count($this->_client->listFiles());
        $this->assertNotEmpty($totalFilesCount);

        // new token should not have access to any files
        $newTokenClient = new \Keboola\StorageApi\Client(array(
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL
        ));
        $this->assertEmpty($newTokenClient->listFiles());

        $newFileId = $this->createAndWaitForFile($filePath, $uploadOptions, $newTokenClient);
        $files = $newTokenClient->listFiles();
        $this->assertCount(1, $files);
        $this->assertEquals($newFileId, reset($files)['id']);

        // new file should be visible for master token
        $files = $this->_client->listFiles();
        $this->assertEquals($newFileId, reset($files)['id']);

        $this->_client->dropToken($newTokenId);

        // new token wil all bucket permissions
        $newTokenId = $this->_client->createToken(array(), 'files manage', null, true);
        $newToken = $this->_client->getToken($newTokenId);


        $this->_client->dropToken($newTokenId);
    }

    public function testFilesPermissionsCanReadAllFiles()
    {
        $uploadOptions = new FileUploadOptions();
        $uploadOptions->setFileName('test.txt')
            ->setFederationToken(true);
        $file = $this->_client->prepareFileUpload($uploadOptions);


        $newTokenId = $this->_client->createToken(array(), 'Files test', null, true);
        $newToken = $this->_client->getToken($newTokenId);

        // new token should not have access to any files
        $newTokenClient = new \Keboola\StorageApi\Client(array(
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL
        ));

        $file = $newTokenClient->getFile($file['id']);
        $this->assertNotEmpty($file);

        $this->_client->updateToken($newTokenId, array(), null, false);

        $token = $this->_client->getToken($newTokenId);
        $this->assertFalse($token['canReadAllFileUploads']);

        try {
            $newTokenClient->getFile($file['id']);
            $this->fail('Access to file should be denied');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        $files = $newTokenClient->listFiles();
        $this->assertEmpty($files);
    }

    public function testsDuplicateTagsShouldBeDeduped()
    {
        $uploadOptions = new FileUploadOptions();
        $uploadOptions
            ->setFileName('test.txt')
            ->setFederationToken(true)
            ->setTags(['first', 'first', 'second']);
        $file = $this->_client->prepareFileUpload($uploadOptions);
        $file = $this->_client->getFile($file['id']);
        $this->assertEquals(['first', 'second'], $file['tags']);
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

    public function testTagging()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $initialTags = array('gooddata', 'image');
        $fileId = $this->_client->uploadFile($filePath, (new FileUploadOptions())->setFederationToken(true)->setTags($initialTags));

        $file = $this->_client->getFile($fileId);
        $this->assertEquals($initialTags, $file['tags']);

        $this->_client->deleteFileTag($fileId, 'gooddata');

        $file = $this->_client->getFile($fileId);
        $this->assertEquals(array('image'), $file['tags']);

        $this->_client->addFileTag($fileId, 'new');
        $file = $this->_client->getFile($fileId);
        $this->assertEquals(array('image', 'new'), $file['tags']);

        $this->_client->addFileTag($fileId, 'new');
        $file = $this->_client->getFile($fileId);
        $this->assertEquals(array('image', 'new'), $file['tags'], 'duplicate tag add is ignored');
    }
}
