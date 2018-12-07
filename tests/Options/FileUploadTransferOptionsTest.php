<?php

namespace Keboola\Test\Options;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadTransferOptions;
use Keboola\Test\StorageApiTestCase;

class FileUploadTransferOptionsTest extends StorageApiTestCase
{


    public function testGetDefaults()
    {
        $options = new FileUploadTransferOptions();
        $this->assertEquals(50, $options->getChunkSize());
        $this->assertEquals(10, $options->getMaxRetriesPerChunk());
        $this->assertEquals(20, $options->getSingleFileConcurrency());
        $this->assertEquals(5, $options->getMultiFileConcurrency());
    }

    public function testSetters()
    {
        $options = new FileUploadTransferOptions();

        $options->setChunkSize(10);
        $options->setMaxRetriesPerChunk(11);
        $options->setMultiFileConcurrency(12);
        $options->setSingleFileConcurrency(13);

        $this->assertEquals(10, $options->getChunkSize());
        $this->assertEquals(11, $options->getMaxRetriesPerChunk());
        $this->assertEquals(12, $options->getMultiFileConcurrency());
        $this->assertEquals(13, $options->getSingleFileConcurrency());
    }

    public function testSetInvalidChunkSize()
    {
        $options = new FileUploadTransferOptions();
        try {
            $options->setChunkSize(0);
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid chunk size: '0'", $e->getMessage());
        }
        try {
            $options->setChunkSize(-1);
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid chunk size: '-1'", $e->getMessage());
        }
        try {
            $options->setChunkSize("abcd");
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid chunk size: 'abcd'", $e->getMessage());
        }
    }

    public function testSetInvalidMaxRetriesPerChunk()
    {
        $options = new FileUploadTransferOptions();
        try {
            $options->setMaxRetriesPerChunk(0);
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid max retries per chunk: '0'", $e->getMessage());
        }
        try {
            $options->setMaxRetriesPerChunk(-1);
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid max retries per chunk: '-1'", $e->getMessage());
        }
        try {
            $options->setMaxRetriesPerChunk("abcd");
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid max retries per chunk: 'abcd'", $e->getMessage());
        }
    }

    public function testSetInvalidMultiFileConcurrency()
    {
        $options = new FileUploadTransferOptions();
        try {
            $options->setMultiFileConcurrency(0);
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid multi file concurrency: '0'", $e->getMessage());
        }
        try {
            $options->setMultiFileConcurrency(-1);
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid multi file concurrency: '-1'", $e->getMessage());
        }
        try {
            $options->setMultiFileConcurrency("abcd");
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid multi file concurrency: 'abcd'", $e->getMessage());
        }
    }

    public function testSetInvalidSingleFileConcurrency()
    {
        $options = new FileUploadTransferOptions();
        try {
            $options->setSingleFileConcurrency(0);
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid single file concurrency: '0'", $e->getMessage());
        }
        try {
            $options->setSingleFileConcurrency(-1);
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid single file concurrency: '-1'", $e->getMessage());
        }
        try {
            $options->setSingleFileConcurrency("abcd");
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid single file concurrency: 'abcd'", $e->getMessage());
        }
    }
}
