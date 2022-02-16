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

function completions(text)
	text = string(text)
    join(completion_text.(REPLCompletions.completions(text, length(text))[1]), " ")
end

# Create MIState to work with history. This works with julia 1.0 to 1.6
term = TerminalBuffer(IOBuffer())
repl = LineEditREPL(term, false)
repl.history_file = true
interface = REPL.setup_interface(repl)
mistate = LineEdit.init_state(term, interface)
prompt_state = LineEdit.state(mistate)
hist = LineEdit.mode(mistate).hist

function history_item(index)
	index = parse(Int, index)
	hist.history[end + 1 - index]
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
	@async begin
		server = listen(getaddrinfo(host), port)
		while true
			sock = accept(server)
			data = String(readavailable(sock))
			request, arg = split(data, ";;"; limit=2)			
			handler = get(handlers, request, nothing)
			if handler === nothing
				close(sock)
				continue
			end
			response = handler(arg)
			try
				write(sock, response * "\n")
				flush(sock)
			catch
			finally
				close(sock)
			end
		end
	end
end

function ping(host, port)
	s = connect(host, port)
    close(s)
end	

end  # module


