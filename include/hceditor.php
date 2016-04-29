<?php 

$hceditor = <<<EOF
<div style="border:2px solid #000; padding: 5px;">

	<div id="edit_info">
		<p><span class="badge">NEW!</span> <strong>Edit your data.</strong>
		You can edit the data points you see displayed on this chart by
		clicking on an individual data point. An edit interface will appear
		here and you can save the change to the central database.</p>
	</div>
	<div style="display:none;" id="edit_div">
	<form name="editor">
	<table class="table table-condensed">
	<tr><td><i class="glyphicon glyphicon-pencil"></i> Data Editor</td>
	<th>Plot:</th><td><span id="edit_plot"></span></td>
	<th>Timestamp:</th><td><span id="edit_time"></span></td>
	<th>Value:</th>
			<td><input type="text" name="edit_value" id="edit_value"></td>
	<th>Options:</th>
		<td>
		<button id="edit_delete" type="button" class="btn btn-default"><i class="glyphicon glyphicon-remove"></i> Delete</button>
		<button id="edit_save" type="button" class="btn btn-default"><i class="glyphicon glyphicon-floppy-save"></i> Save Edit</button>
		</td>
	</tr>
	</table>
	<strong>Optionally, add a comment about this change:</strong>
	<br /><textarea id="edit_comment" cols="80"></textarea>
	<br />Explaination of Options:<br />
	<ul>
		<li><strong>Delete:</strong> Sets the quality controlled value in the
		database to null.  The original value is retained.</li>
		<li><strong>Save Edit:</strong> Overwrite the quality controlled value
		saved in the database.</li>
	</ul>
	</form>
	</div>
	<div id="edit_waiting" style="display: none;"><img src="/images/wait24trans.gif"> Sending Edit to Server</div>
	<ul id="edit_log"></ul>
</div>
EOF;

?>