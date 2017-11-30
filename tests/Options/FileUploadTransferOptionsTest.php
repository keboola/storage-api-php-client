<?php

namespace Keboola\Test\Options;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadTransferOptions;
use Keboola\Test\StorageApiTestCase;

class FileUploadTransferOptionsTest extends StorageApiTestCase
{

    public function testSetChunkSize()
    {
        $options = new FileUploadTransferOptions();
        $options->setChunkSize(10);
        $this->assertEquals(10, $options->getChunkSize());
    }

    public function testGetDefaultChunkSize()
    {
        $options = new FileUploadTransferOptions();
        $this->assertEquals(50, $options->getChunkSize());
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

    public function testSetMaxRetriesPerChunk()
    {
        $options = new FileUploadTransferOptions();
        $options->setMaxRetriesPerChunk(10);
        $this->assertEquals(10, $options->getMaxRetriesPerChunk());
    }

    public function testGetDefaultMaxRetriesPerChunk()
    {
        $options = new FileUploadTransferOptions();
        $this->assertEquals(50, $options->getMaxRetriesPerChunk());
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
}
