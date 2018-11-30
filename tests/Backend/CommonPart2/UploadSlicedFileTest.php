<?php

namespace Keboola\Test\Backend\CommonPart2;

use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

use Keboola\Csv\CsvFile;

class UploadSlicedFileTest extends StorageApiTestCase
{
    public function testNoCompress()
    {
        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('entries')
            ->setIsEncrypted(true)
            ->setIsSliced(true)
            ->setCompress(false);
        $slices = [
            __DIR__ . '/../../_data/sliced/neco_0000_part_00',
        ];
        $slicedFileId = $this->_client->uploadSlicedFile($slices, $uploadOptions);
        $slicedFile = $this->_client->getFile($slicedFileId);

        $this->assertEquals(filesize(__DIR__ . '/../../_data/sliced/neco_0000_part_00'), $slicedFile['sizeBytes']);
        $this->assertEquals('entries', $slicedFile['name']);
    }

    public function testCompress()
    {
        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('entries')
            ->setIsEncrypted(true)
            ->setIsSliced(true)
            ->setCompress(true);
        $slices = [
            __DIR__ . '/../../_data/sliced/neco_0000_part_00',
        ];
        $slicedFileId = $this->_client->uploadSlicedFile($slices, $uploadOptions);
        $slicedFile = $this->_client->getFile($slicedFileId);

        $this->assertLessThan(filesize(__DIR__ . '/../../_data/sliced/neco_0000_part_00'), $slicedFile['sizeBytes']);
        $this->assertEquals('entries.gz', $slicedFile['name']);
    }
}
