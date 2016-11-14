<?php
/**
 *
 * Test if an error message from API raises a ClientException
 *
 * User: Ondrej Hlavacek
 * Date: 11.12.12
 * Time: 17:22 PST
 *
 */
namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;

class ExceptionsTest extends StorageApiTestCase
{
    /**
     * @expectedException \Keboola\StorageApi\ClientException
     */
    public function testException()
    {
        $t = $this->_client->getTable("nonexistingtable");
    }
}
