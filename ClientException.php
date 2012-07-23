<?
namespace Keboola\StorageApi;
class ClientException extends \Exception
{
	/**
	 * @var null|Exception
	 */
	private $_previous = null;

	/**
	 * Construct the exception
	 *
	 * @param  string $msg
	 * @param  int $code
	 * @param  Exception $previous
	 * @return void
	 */
	public function __construct($msg = '', $code = 0, Exception $previous = null)
	{
		if (version_compare(PHP_VERSION, '5.3.0', '<')) {
			parent::__construct($msg, (int) $code);
			$this->_previous = $previous;
		} else {
			parent::__construct($msg, (int) $code, $previous);
		}
	}
}