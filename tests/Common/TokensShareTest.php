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

class TokensShareTest extends StorageApiTestCase
{

    public function testMasterTokenShouldNotBeShareable()
    {
        try {
            $token = $this->_client->verifyToken();
            $this->_client->shareToken($token['id'], 'test@devel.keboola.com', 'Hi');
            $this->fail('Master token should not be shareable');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.token.cannotShareMasterToken', $e->getStringCode());
        }
    }

    public function testTokenShare()
    {
        $newTokenId = $this->_client->createToken(array());
        $this->_client->shareToken($newTokenId, 'test@devel.keboola.com', 'Hi');
    }
}
