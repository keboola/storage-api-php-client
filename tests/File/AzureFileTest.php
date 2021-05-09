<?php

namespace Keboola\Test\File;

use GuzzleHttp\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Test\StorageApiTestCase;

class AzureFileTest extends StorageApiTestCase
{
    /**
     * @dataProvider uploadData
     */
    public function testFileUpload($filePath, FileUploadOptions $options)
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
        $client->get($file['url']);
    }

    public function uploadData()
    {
        $path = __DIR__ . '/../_data/files.upload.txt';
        $this->generateFile(sys_get_temp_dir() . '/large_abs_upload.txt', 50);

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
            'large file' => array(
                sys_get_temp_dir() . '/large_abs_upload.txt',
                (new FileUploadOptions())
                    ->setIsPublic(true)
                    ->setIsPermanent(true)
                    ->setTags(array('sapi-import', 'martin'))
            ),
        );
    }

    public function uploadSlicedData()
    {
        $part1 = sys_get_temp_dir() . '/slice.csv.part_1';
        $part2 = sys_get_temp_dir() . '/slice.csv.part_2';
        $parts = [$part1, $part2];
        $this->generateFile($part1, 100);
        $this->generateFile($part2, 100);

        return [
            [
                $parts,
                (new FileUploadOptions())
                    ->setIsSliced(true)
                    ->setFileName("slice.csv")
            ]
        ];
    }

    /**
     * @dataProvider uploadSlicedData
     */
    public function testUploadSlicedFile(array $slices, FileUploadOptions $options)
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
        $manifest = json_decode(file_get_contents($file['url']), true);
        $this->assertCount(count($slices), $manifest["entries"]);
    }

    /**
     * @param string $largePath
     * @param int $fileSizeMegabytes
     */
    private function generateFile($largePath, $fileSizeMegabytes)
    {
        if (file_exists($largePath)) {
            unlink($largePath);
        }
        $fp = fopen($largePath, 'a+');
        $i = 0;
        while ($i++ < $fileSizeMegabytes) {
            fwrite($fp, str_repeat('X', 1024 * 1024));
        }
        fclose($fp);
    }
}
