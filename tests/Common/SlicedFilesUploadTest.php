<?php
/**
 *
 * User: Ondřej Hlaváček
 *
 */

namespace Keboola\Test\Common;

use Keboola\StorageApi\Options\FileUploadTransferOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Test\StorageApiTestCase;
use \Keboola\StorageApi\Options\FileUploadOptions;

class SlicedFilesUploadTest extends StorageApiTestCase
{
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

        $s3Client = new \Aws\S3\S3Client([
            'version' => '2006-03-01',
            'region' => $file['region'],
            'credentials' => [
                'key' => $file['credentials']['AccessKeyId'],
                'secret' => $file['credentials']['SecretAccessKey'],
                'token' => $file['credentials']['SessionToken'],
            ]
        ]);

        foreach ($slices as $filePath) {
            $object = $s3Client->getObject([
                'Bucket' => $file['s3Path']['bucket'],
                'Key' => $file["s3Path"]["key"] . basename($filePath)
            ]);
            $this->assertEquals(file_get_contents($filePath), $object['Body']);
        }

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

    public function testUploadSlicedFileChunks()
    {
        $parts = 50;
        $slices = [];
        for ($i = 0; $i < $parts; $i++) {
            $tempfile = tempnam(sys_get_temp_dir(), 'sapi-client-test-slice-' . $i);
            $file = new \Keboola\Csv\CsvFile($tempfile);
            $file->writeRow(["row" . $i, "value" . $i]);
            $slices[] = $tempfile;
        }
        $fileUploadOptions = (new FileUploadOptions())
            ->setIsSliced(true)
            ->setFileName("manyparts.csv");
        $fileUploadTransferOptions = (new FileUploadTransferOptions())
            ->setChunkSize(20);
        $fileId = $this->_client->uploadSlicedFile($slices, $fileUploadOptions, $fileUploadTransferOptions);
        $file = $this->_client->getFile($fileId, (new GetFileOptions())->setFederationToken(true));

        $fileSize = 0;
        foreach ($slices as $filePath) {
            $fileSize += filesize($filePath);
        }
        $this->assertEquals($fileSize, $file['sizeBytes']);
        $manifest = json_decode(file_get_contents($file['url']), true);
        $this->assertCount(count($slices), $manifest["entries"]);

        $s3Client = new \Aws\S3\S3Client([
            'version' => '2006-03-01',
            'region' => $file['region'],
            'credentials' => [
                'key' => $file['credentials']['AccessKeyId'],
                'secret' => $file['credentials']['SecretAccessKey'],
                'token' => $file['credentials']['SessionToken'],
            ]
        ]);

        foreach ($slices as $filePath) {
            $object = $s3Client->getObject([
                'Bucket' => $file['s3Path']['bucket'],
                'Key' => $file["s3Path"]["key"] . basename($filePath)
            ]);
            $this->assertEquals(file_get_contents($filePath), $object['Body']);
        }
    }

    public function uploadSlicedData()
    {
        $part1 = __DIR__ . '/../_data/languages.csv.part_1';
        $part2 = __DIR__ . '/../_data/languages.csv.part_2';
        $parts = [$part1, $part2];

        return array(
            array(
                $parts,
                (new FileUploadOptions())
                    ->setIsSliced(true)
                    ->setFileName("languages.csv")
            ),
            array(
                $parts,
                (new FileUploadOptions())
                    ->setIsEncrypted(false)
                    ->setIsSliced(true)
                    ->setFileName("languages.csv")
            ),
            array(
                $parts,
                (new FileUploadOptions())
                    ->setNotify(false)
                    ->setCompress(false)
                    ->setIsPublic(false)
                    ->setIsSliced(true)
                    ->setFileName("languages.csv")
            ),
            array(
                $parts,
                (new FileUploadOptions())
                    ->setIsPermanent(true)
                    ->setTags(array('sapi-import', 'martin'))
                    ->setIsSliced(true)
                    ->setFileName("languages.csv")
            ),
        );
    }
}
