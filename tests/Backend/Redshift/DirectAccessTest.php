<?php

namespace Keboola\Test\Backend\Redshift;

use Keboola\StorageApi\DirectAccess;
use Keboola\Test\StorageApiTestCase;

class DirectAccessTest extends StorageApiTestCase
{
    public function testGetDirectAccessCredentials()
    {
        $backend = self::BACKEND_REDSHIFT;
        $directAccess = new DirectAccess($this->_client);

        try {
            $directAccess->createCredentials(self::BACKEND_SNOWFLAKE);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(
                'storage.directAccess.projectNotSupportRequireBackend',
                $e->getStringCode()
            );
        }

        try {
            $directAccess->createCredentials($backend);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('Invalid request', $e->getMessage());
            $this->assertEquals(
                'storage.directAccess.notSupportedBackendForDirectAccess',
                $e->getStringCode()
            );
        }
    }
}
