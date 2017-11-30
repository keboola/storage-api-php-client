
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
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\StorageApiTestCase;

class FileUploadOptionsTest extends StorageApiTestCase
{

    public function testSetMultipartUploadThreshold()
    {
        $options = new FileUploadOptions();
        $options->setMultipartUploadThreshold(10);
        $this->assertEquals(10, $options->getMultipartUploadThreshold());
    }

    public function testGetDefaultMultipartUploadThreshold()
    {
        $options = new FileUploadOptions();
        $this->assertEquals(104857600, $options->getMultipartUploadThreshold());
    }

    public function testSetInvalidMultipartUploadThreshold()
    {
        $options = new FileUploadOptions();
        try {
            $options->setMultipartUploadThreshold(0);
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid multipart upload threshold size: '0'", $e->getMessage());
        }
        try {
            $options->setMultipartUploadThreshold(-1);
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid multipart upload threshold size: '-1'", $e->getMessage());
        }
        try {
            $options->setMultipartUploadThreshold("abcd");
            $this->fail("Exception not caught.");
        } catch (ClientException $e) {
            $this->assertEquals("Invalid multipart upload threshold size: 'abcd'", $e->getMessage());
        }
    }
}
