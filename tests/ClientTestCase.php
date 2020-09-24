<?php

namespace Keboola\Test;

use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class ClientTestCase extends TestCase
{
    /**
     * @return Client
     */
    protected function getClient(array $options)
    {
        $buildId = '';
        if (getenv('TRAVIS_BUILD_ID')) {
            $buildId = sprintf('Build id: ' . getenv('TRAVIS_BUILD_ID'));
        }

        $options['userAgent'] = sprintf('%s Test: %s', $buildId, $this->getTestName());
        return new Client($options);
    }

    /**
     * @return string
     */
    protected function getTestName()
    {
        return get_class($this) . '//' . $this->getName();
    }
}
