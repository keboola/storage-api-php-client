<?php

namespace Keboola\Test\File;

use GuzzleHttp\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\StorageApiTestCase;

class AzureFileTest extends StorageApiTestCase
{
    /**
     * @dataProvider uploadData
     */
    public function testFileUpload($filePath, FileUploadOptions $options)
    {
        $fileId = $this->_client->uploadFile($filePath, $options);
        $file = $this->_client->getFile($fileId);

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
            $this->assertEquals(180, $file['maxAgeDays']);
        }

        // check attachment, download
        $client = new Client();
        $client->get($file['url']);
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
}

