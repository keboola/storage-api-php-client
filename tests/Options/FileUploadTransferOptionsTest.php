<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

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
}
