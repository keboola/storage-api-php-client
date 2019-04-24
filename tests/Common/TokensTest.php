<?php
namespace Keboola\Test\Common;

use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\StorageApiTestCase;

class TokensTest extends StorageApiTestCase
{
    public function testTokenRefresh()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token')
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $tokenString = $token['token'];
        $created = new \DateTime($token['created']);

        sleep(1);

        $this->_client->refreshToken($tokenId);
        $token = $this->_client->getToken($tokenId);

        $refreshed = new \DateTime($token['refreshed']);

        $this->assertNotEquals($tokenString, $token['token']);
        $this->assertGreaterThan($created->getTimestamp(), $refreshed->getTimestamp());
    }
}
