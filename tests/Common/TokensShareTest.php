<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\EventTesterUtils;

class TokensShareTest extends StorageApiTestCase
{
    use EventTesterUtils;

    public function testMasterTokenShouldNotBeShareable(): void
    {
        try {
            $token = $this->_client->verifyToken();
            $this->tokens->shareToken($token['id'], 'test@devel.keboola.com', 'Hi');
            $this->fail('Master token should not be shareable');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.token.cannotShareMasterToken', $e->getStringCode());
        }
    }

    public function testTokenShare(): void
    {
        $this->initEvents($this->_client);
        $newToken = $this->tokens->createToken(new TokenCreateOptions());
        $this->tokens->shareToken($newToken['id'], 'test@devel.keboola.com', 'Hi');

        $assertCallback = function ($events) {
            $this->assertGreaterThanOrEqual(1, count($events));
            $this->assertSame('storage.tokenShared', $events[0]['event']);
            $this->assertSame('test@devel.keboola.com', $events[0]['params']['recipientEmail']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tokenShared')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);
    }
}
