<?php
/**
 * Storage API Client - Table Exporter
 *
 *
 *
 * @author Ondrej Hlavacek <ondrej.hlavacek@keboola.com>
 * @date: 5.6.14
 */

namespace Keboola\StorageApi;

use Aws\S3\S3Client;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class TableExporter
{
	/**
	 * @var Client
	 */
	private $client;

	/**
	 * @param Client $client
	 */
	public function __construct(Client $client) {
		$this->client = $client;
	}

	/**
	 *
	 * Process async export and prepare the file on disk
	 *
	 * @param $tableId SAPI Table Id
	 * @param $destination destination file
	 * @param $exportOptions SAPI Client export options
	 * @return void
	 */
	public function exportTable($tableId, $destination, $exportOptions)
	{
		if (!isset($exportOptions['gzip'])) {
			$exportOptions['gzip'] = false;
		}

		$table = $this->client->getTable($tableId);
		$fileId = $this->client->exportTableAsync($tableId, $exportOptions);

		$fileInfo = $this->client->getFile($fileId["file"]["id"], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));
		$s3Client = S3Client::factory(array(
			"key" => $fileInfo["credentials"]["AccessKeyId"],
			"secret" => $fileInfo["credentials"]["SecretAccessKey"],
			"token" => $fileInfo["credentials"]["SessionToken"]
		));

		$tmpFilePath = sys_get_temp_dir() . '/sapi-php-client' . '/' . uniqid('sapi-export-');

		$fs = new Filesystem();

		if ($fileInfo['isSliced'] === true) {
			/**
			 * sliced file - combine files together
			 */
			$manifest = json_decode(file_get_contents($fileInfo["url"]), true);
			$files = array();
			foreach($manifest["entries"] as $part) {
				$fileKey = substr($part["url"], strpos($part["url"], '/', 5) + 1);
				$filePath = $tmpFilePath . '_' . md5(str_replace('/', '_', $fileKey));
				$files[] = $filePath;
				$s3Client->getObject(array(
					'Bucket' => $fileInfo["s3Path"]["bucket"],
					'Key'    => $fileKey,
					'SaveAs' => $filePath
				));
			}

			$header = '"' . join($table["columns"], '","') . '"' . "\n";
			if ($exportOptions["gzip"] === true) {
				$fs->dumpFile($destination . '.tmp', $header);
			} else {
				$fs->dumpFile($destination, $header);
			}

			foreach($files as $file) {
				if ($exportOptions["gzip"]) {
					$catCmd = "gunzip " . escapeshellarg($file) . " --to-stdout >> " . escapeshellarg($destination) . ".tmp";
				} else {
					$catCmd = "cat $file >> $destination";
				}
				(new Process($catCmd))->mustRun();
				$fs->remove($file);
			}
			if ($exportOptions["gzip"]) {
				$gZipCmd = "gzip " . escapeshellarg($destination) . ".tmp --fast";
				(new Process($gZipCmd))->mustRun();
				$fs->rename($destination.'.tmp.gz', $destination);
			}

		} else {
			/**
			 * NonSliced file, just move
			 */
			$s3Client->getObject(array(
				'Bucket' => $fileInfo["s3Path"]["bucket"],
				'Key'    => $fileInfo["s3Path"]["key"],
				'SaveAs' => $tmpFilePath
			));
			$fs->rename($tmpFilePath, $destination);
		}

		return;
	}
}
