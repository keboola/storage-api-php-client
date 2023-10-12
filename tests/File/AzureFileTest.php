<?php

namespace Keboola\Test\File;

use Exception;
use GuzzleHttp\Client;
use Keboola\StorageApi\ABSUploader;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Test\StorageApiTestCase;
use MicrosoftAzure\Storage\Blob\Models\CommitBlobBlocksOptions;
use MicrosoftAzure\Storage\Blob\Models\CreateBlockBlobOptions;

class AzureFileTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $token = $this->_client->verifyToken();
        $this->assertSame(
            'azure',
            $token['owner']['fileStorageProvider'],
            'Project must have ABS file storage'
        );
    }

    /**
     * @dataProvider uploadData
     */
    public function testFileUpload($filePath, FileUploadOptions $options): void
    {
        $fileId = $this->_client->uploadFile($filePath, $options);
        $file = $this->_client->getFile($fileId, (new GetFileOptions())->setFederationToken(true));

        //azure storage is always encrypted and private. Request params 'isEncrypted' and 'isPublic' is ignored
        $this->assertFalse($file['isPublic']);
        $this->assertTrue($file['isEncrypted']);

        $this->assertEquals(basename($filePath), $file['name']);
        $this->assertEquals(filesize($filePath), $file['sizeBytes']);
        $this->assertArrayHasKey('absCredentials', $file);
        $this->assertArrayHasKey('absPath', $file);
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

    public function uploadData()
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

    public function uploadSlicedData()
    {
        $part1 = sys_get_temp_dir() . '/slice.csv.part_1';
        $part2 = sys_get_temp_dir() . '/slice.csv.part_2';
        $parts = [$part1, $part2];
        $this->generateFile($part1, 16);
        $this->generateFile($part2, 16);

        return [
            [
                $parts,
                (new FileUploadOptions())
                    ->setIsSliced(true)
                    ->setFileName('slice.csv'),
            ],
        ];
    }

    public function testReUpload(): void
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $options = new FileUploadOptions();
        $options
            ->setFileName('upload.txt')
            ->setFederationToken(true)
            ->setIsEncrypted(false);

        $prepareResult = $this->_client->prepareFileUpload($options);

        $blobClient = BlobClientFactory::createClientFromConnectionString(
            $prepareResult['absUploadParams']['absCredentials']['SASConnectionString']
        );

        $parallel = true;
        $options = new CommitBlobBlocksOptions();
        if (!$prepareResult['sizeBytes']) {
            // cannot upload empty file in parallel, needs to be created directly
            $options = new CreateBlockBlobOptions();
            $parallel = false;
        }
        $options->setContentDisposition(
            sprintf('attachment; filename=%s', $prepareResult['name'])
        );

        $uploader = new ABSUploader($blobClient);
        $uploader->uploadFile(
            $prepareResult['absUploadParams']['container'],
            $prepareResult['absUploadParams']['blobName'],
            $filePath,
            $options,
            $parallel
        );

        // re-upload should work
        $uploader->uploadFile(
            $prepareResult['absUploadParams']['container'],
            $prepareResult['absUploadParams']['blobName'],
            $filePath,
            $options,
            $parallel
        );
    }

    /**
     * @dataProvider uploadSlicedData
     */
    public function testUploadSlicedFile(array $slices, FileUploadOptions $options): void
    {
        $fileId = $this->_client->uploadSlicedFile($slices, $options);
        $file = $this->_client->getFile($fileId, (new GetFileOptions())->setFederationToken(true));

        $this->assertEquals($options->getIsPublic(), $file['isPublic']);
        $this->assertEquals($options->getFileName(), $file['name']);
        $fileSize = 0;
        foreach ($slices as $filePath) {
            $fileSize += filesize($filePath);
        }
        $this->assertEquals($fileSize, $file['sizeBytes']);
        sleep(1); // tests sometimes return 403, but file can be downloaded just a few seconds later
        $manifest = json_decode(file_get_contents($file['url']), true);
        $this->assertCount(count($slices), $manifest['entries']);
    }

    public function testDeleteNonUploadedSlicedFile(): void
    {
        $options = new FileUploadOptions();
        $options
            ->setFileName('entries_')
            ->setFederationToken(true)
            ->setIsSliced(true)
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

    /**
     * @param string $filepath
     * @param int $fileSizeMegabytes
     */
    private function generateFile($filepath, $fileSizeMegabytes)
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
