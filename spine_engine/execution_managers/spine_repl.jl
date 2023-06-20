######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

module SpineREPL

using Sockets
using REPL
using REPL.REPLCompletions
using REPL.Terminals
using REPL.LineEdit

_exception = false
# History related stuff. This works with julia 1.0 to 1.6 at least
term = TTYTerminal("", stdin, IOBuffer(), stderr)
repl = LineEditREPL(term, false)
repl.history_file = true
interface = REPL.setup_interface(repl)
mistate = LineEdit.init_state(term, interface)
prompt_state = LineEdit.state(mistate)
prompt = LineEdit.mode(prompt_state)
hist = prompt.hist
search_state = LineEdit.init_state(term, LineEdit.PrefixHistoryPrompt(hist, prompt))

function set_exception(value)
	global _exception = value
end

function ping(host, port)
	s = connect(host, port)
    write(s, _exception ? "error" : "ok")
    close(s)
    REPL.history_reset_state(hist)
end	

function completions(text)
	text = string(text)
    join(completion_text.(REPLCompletions.completions(text, length(text))[1]), " ")
end

function history_item(text, prefix, sense)
	backwards = sense == "backwards"
	take!(search_state.response_buffer)
    write(search_state.response_buffer, text)
	REPL.history_move_prefix(search_state, hist, prefix, backwards)
	LineEdit.input_string(search_state)
end

function add_history(line)
	line = string(line)
    take!(prompt_state.input_buffer)
    write(prompt_state.input_buffer, line)
    REPL.add_history(hist, prompt_state)
end

function is_complete(cmd)
	cmd = string(cmd)
	try
		_is_complete(Meta.parse(cmd))
	catch
		"true"
	end
end

_is_complete(expr::Expr) = (expr.head === :incomplete) ? "false" : "true"
_is_complete(other) = "true"

function start_server(host, port)
	handlers = Dict(
		"completions" => completions,
		"add_history" => add_history,
		"history_item" => history_item,
		"is_complete" => is_complete
	)
	req_args_sep = '\u1f'  # Unit separator
	args_sep = '\u91'  # Private Use 1
	@async begin
		server = listen(getaddrinfo(host), port)
		while true
			sock = accept(server)
			data = String(readavailable(sock))
			request, args = split(data, req_args_sep; limit=2)
			args = split(args, args_sep)	
			handler = get(handlers, request, nothing)
			if handler === nothing
				close(sock)
				continue
			end
			response = handler(args...)
			try
				write(sock, response * "\n")
				flush(sock)
			catch
			end
		end
	end
end

end  # module


