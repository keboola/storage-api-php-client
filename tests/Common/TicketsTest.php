<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Event;

class TicketsTest extends StorageApiTestCase
{

    public function testGenerator()
    {
        $id1 = $this->_client->generateId();
        $this->assertNotEmpty($id1);

        $id2 = $this->_client->generateId();
        $this->assertGreaterThan($id1, $id2);
    }
}
