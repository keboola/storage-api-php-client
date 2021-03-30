<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\StorageApiTestCase;

class TokensShareTest extends StorageApiTestCase
{

    public function testMasterTokenShouldNotBeShareable()
    {
        try {
            $token = $this->_client->verifyToken();
            $this->tokens->shareToken($token['id'], 'test@devel.keboola.com', 'Hi');
            $this->fail('Master token should not be shareable');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.token.cannotShareMasterToken', $e->getStringCode());
        }
    }

    public function testTokenShare()
    {
        $newToken = $this->tokens->createToken(new TokenCreateOptions());
        $this->tokens->shareToken($newToken['id'], 'test@devel.keboola.com', 'Hi');
    }
}
