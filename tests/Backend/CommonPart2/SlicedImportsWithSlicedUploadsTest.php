<?php
/**
 *
 * User: Ondřej Hlaváček
 *
 */

namespace Keboola\Test\Backend\CommonPart2;

use Keboola\Test\StorageApiTestCase;

use Keboola\Csv\CsvFile;

class SlicedImportsWithSlicedUploadsTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testSlicedImportGzipped()
    {

        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('entries_')
            ->setIsEncrypted(true)
            ->setIsSliced(true);
        $slices = [
            __DIR__ . '/../../_data/sliced/neco_0000_part_00.gz',
            __DIR__ . '/../../_data/sliced/neco_0001_part_00.gz'
        ];
        $slicedFileId = $this->_client->uploadSlicedFile($slices, $uploadOptions);

        $headerFile = new CsvFile(__DIR__ . '/../../_data/sliced/header.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'entries', $headerFile);
        $this->_client->writeTableAsyncDirect($tableId, array(
            'dataFileId' => $slicedFileId,
            'columns' => $headerFile->getHeader(),
            'delimiter' => '|',
            'enclosure' => '',
        ));
    }

    public function testSlicedImportSingleFile()
    {
        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('languages_')
            ->setIsSliced(true)
            ->setIsEncrypted(false);
        $slices = [
            __DIR__ . '/../../_data/languages.no-headers.csv'
        ];
        $slicedFileId = $this->_client->uploadSlicedFile($slices, $uploadOptions);

        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'entries', new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        $this->_client->deleteTableRows($tableId);
        $this->_client->writeTableAsyncDirect($tableId, array(
            'dataFileId' => $slicedFileId,
            'delimiter' => ',',
            'enclosure' => '"',
            'escapedBy' => '',
            'columns' => array('id', 'name'),
        ));

        $this->assertLinesEqualsSorted(file_get_contents(__DIR__ . '/../../_data/languages.csv'), $this->_client->exportTable($tableId, null, array(
            'format' => 'rfc',
        )), 'imported data comparsion');

        // incremental
        $this->_client->writeTableAsyncDirect($tableId, array(
            'dataFileId' => $slicedFileId,
            'incremental' => true,
            'delimiter' => ',',
            'enclosure' => '"',
            'escapedBy' => '',
            'columns' => array('id', 'name'),
        ));

        $data = file_get_contents(__DIR__ . '/../../_data/languages.csv');
        $lines = explode("\n", $data);
        array_shift($lines);
        $data = $data . implode("\n", $lines);

        $this->assertLinesEqualsSorted($data, $this->_client->exportTable($tableId, null, array(
            'format' => 'rfc',
        )), 'imported data comparsion');
    }
}
