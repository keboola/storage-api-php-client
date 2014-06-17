<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 25/11/13
 * Time: 10:11
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Aws\Plugin\Backoff;

use Guzzle\Plugin\Backoff\BackoffPlugin as BackoffPluginBase;
use Guzzle\Plugin\Backoff\CurlBackoffStrategy;
use Guzzle\Plugin\Backoff\ExponentialBackoffStrategy;
use Guzzle\Plugin\Backoff\HttpBackoffStrategy;
use Guzzle\Plugin\Backoff\TruncatedBackoffStrategy;
use Aws\Common\Client\ExpiredCredentialsChecker;
use Aws\Common\Client\ThrottlingErrorChecker;
use Aws\Common\Exception\Parser\DefaultXmlExceptionParser;


final class BackoffPlugin
{
	/**
	 * @return BackoffPluginBase
	 */
	public static function factory()
	{
		$exceptionParser = new DefaultXmlExceptionParser();
		return new BackoffPluginBase(
		// Retry failed requests up to 5 times if it is determined that the request can be retried
			new TruncatedBackoffStrategy(5,
				// Retry failed requests with 400-level responses due to throttling
				new ThrottlingErrorChecker($exceptionParser,
					// Retry failed requests due to transient network or cURL problems
					new CurlBackoffStrategy(null,
						// Retry failed requests with 500-level responses
						new HttpBackoffStrategy(array(500, 502, 503, 504, 509),
							// Retry requests that failed due to expired credentials
							new ExpiredCredentialsChecker($exceptionParser,
								new ExponentialBackoffStrategy()
							)
						)
					)
				)
			));
	}

}