<?php

namespace Keboola\Test\File;

use GuzzleHttp\Client;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Test\StorageApiTestCase;

class GcsFileTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $token = $this->_client->verifyToken();
        $this->assertSame(
            'gcp',
            $token['owner']['fileStorageProvider'],
            'Project must have GCS file storage'
        );
    }

    /**
     * @dataProvider uploadData
     */
    public function testFileUpload(string $filePath, FileUploadOptions $options): void
    {
        $fileId = $this->_client->uploadFile($filePath, $options);
        $file = $this->_client->getFile($fileId, (new GetFileOptions())->setFederationToken(true));

        //gcs storage is always encrypted and private. Request params 'isEncrypted' and 'isPublic' is ignored
        $this->assertFalse($file['isPublic']);
        $this->assertTrue($file['isEncrypted']);

        $this->assertEquals(basename($filePath), $file['name']);
        $this->assertEquals(filesize($filePath), $file['sizeBytes']);
        $this->assertArrayHasKey('gcsCredentials', $file);
        $this->assertArrayHasKey('gcsPath', $file);
        sleep(1); // tests sometimes return 403, but file can be downloaded just a few seconds later
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
            $this->assertIsInt($file['maxAgeDays']);
            $this->assertEquals(StorageApiTestCase::FILE_LONG_TERM_EXPIRATION_IN_DAYS, $file['maxAgeDays']);
        }

        // check attachment, download
        $client = new Client();
        $client->get($file['url']);
    }

    /**
     * @dataProvider isSliced
     */
    public function testDeleteNonUploadedSlicedFile(bool $isSliced): void
    {
        $options = new FileUploadOptions();
        $options
            ->setFileName('entries_')
            ->setFederationToken(true)
            ->setIsSliced($isSliced)
            ->setIsEncrypted(true);

        $result = $this->_client->prepareFileUpload($options);

        $fileId = $result['id'];
        $file = $this->_client->getFile($fileId, (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));
        $this->assertNotNull($file);
        $this->_client->deleteFile($fileId);

        $this->expectException(\Keboola\StorageApi\ClientException::class);
        $this->expectExceptionMessage('File not found');
        $this->_client->getFile($fileId, (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));
    }

    public function isSliced(): array
    {
        return [
            [false],
            [true],
        ];
    }

    public function uploadData(): array
    {
        $path = __DIR__ . '/../_data/files.upload.txt';
        $largeFilePath = sys_get_temp_dir() . '/large_abs_upload.txt';
        $this->generateFile($largeFilePath, 16);

        return [
            [
                $path,
                (new FileUploadOptions())->setIsPublic(true),
            ],
            [
                $path,
                (new FileUploadOptions())
                    ->setIsPublic(true),
            ],
            [
                $path,
                (new FileUploadOptions())
                    ->setIsEncrypted(false),
            ],
            [
                $path,
                (new FileUploadOptions())
                    ->setIsEncrypted(true),
            ],
            [
                $path,
                (new FileUploadOptions())
                    ->setNotify(false)
                    ->setCompress(false)
                    ->setIsPublic(false),
            ],
            [
                $path,
                (new FileUploadOptions())
                    ->setIsPublic(true)
                    ->setIsPermanent(true)
                    ->setTags(['sapi-import', 'martin']),
            ],
            'large file' => [
                $largeFilePath,
                (new FileUploadOptions())
                    ->setIsPublic(true)
                    ->setIsPermanent(true)
                    ->setTags(['sapi-import', 'martin']),
            ],
        ];
    }

    private function generateFile(string $filepath, int $fileSizeMegabytes): void
    {
        if (file_exists($filepath)) {
            unlink($filepath);
        }
        $fp = fopen($filepath, 'a+');
        if ($fp === false) {
            throw new Exception(sprintf('Cannot open file "%s"', $filepath));
        }
        $i = 0;
        while ($i++ < $fileSizeMegabytes) {
            fwrite($fp, str_repeat('X', 1024 * 1024));
        }
        fclose($fp);
    }
}
