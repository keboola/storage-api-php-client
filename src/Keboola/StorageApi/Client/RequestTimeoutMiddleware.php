<?php

namespace Keboola\StorageApi\Client;

use Keboola\StorageApi\Client;
use Psr\Http\Message\RequestInterface;
use Psr\Log\LoggerInterface;
use function GuzzleHttp\Psr7\str;

class RequestTimeoutMiddleware
{
    const REQUEST_TIMEOUT_DEFAULT = 120;
    const REQUEST_TIMEOUT_EXTENDED = 7200;

    /** @var LoggerInterface */
    private $logger;

    /** @var callable */
    private $nextHandler;

    private function __construct(
        callable $nextHandler,
        LoggerInterface $logger
    ) {
        $this->logger = $logger;
        $this->nextHandler = $nextHandler;
    }

    /**
     * @return callable
     */
    public static function factory(LoggerInterface $logger)
    {
        return function (callable $handler) use ($logger) {
            return new self($handler, $logger);
        };
    }

    /**
     * @return mixed
     */
    public function __invoke(
        RequestInterface $request,
        array $options
    ) {
        $nextHander = $this->nextHandler;

        $isExtendedTimeout = array_key_exists(Client::REQUEST_OPTION_EXTENDED_TIMEOUT, $options) ? (bool) $options[Client::REQUEST_OPTION_EXTENDED_TIMEOUT] : false;
        if ($request->getMethod() === 'DELETE') {
            $isExtendedTimeout = true;
        }

        $options['timeout'] = self::REQUEST_TIMEOUT_DEFAULT;
        if ($isExtendedTimeout) {
            $options['timeout'] = self::REQUEST_TIMEOUT_EXTENDED;
        }
        $this->logger->debug(sprintf('Request "%s %s" timeout set to "%ss"', $request->getMethod(), $request->getUri()->getPath(), $options['timeout']));
        return $nextHander($request, $options);
    }
}
