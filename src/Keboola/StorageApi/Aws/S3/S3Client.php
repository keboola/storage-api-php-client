<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 10/14/13
 * Time: 3:15 PM
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Aws\S3;

use Aws\S3\S3Client as S3ClientBase,
	Aws\Common\Enum\ClientOptions;

class S3Client
{
	/**
	 * @param array $config
	 * @return S3ClientBase
	 */
	public static function factory($config = array())
	{
		$config[ClientOptions::BACKOFF] = \Keboola\StorageApi\Aws\Plugin\Backoff\BackoffPlugin::factory();
		return S3ClientBase::factory($config);
	}

}